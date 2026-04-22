from __future__ import annotations

import asyncio
import hashlib
import json
import random
import re
import shutil
from copy import deepcopy
from datetime import datetime
from pathlib import Path
from typing import Any

import astrbot.api.message_components as Comp
from astrbot.api import AstrBotConfig, logger
from astrbot.api.event import AstrMessageEvent, filter
from astrbot.api.star import Context, Star, register
from astrbot.core.utils.session_waiter import SessionController, session_waiter

PLUGIN_NAME = "astrbot_plugin_echo_cave"
PLUGIN_VERSION = "1.4.0"
DATA_FILE_NAME = "echo_cave_data.json"
LIST_LIMIT = 20
SUMMARY_LIMIT = 24
SUBMIT_TIMEOUT_SECONDS = 60
ADMIN_ROLE_KEYWORDS = {"admin", "administrator", "owner", "superadmin", "super_admin"}
CANCEL_SUBMIT_WORDS = {"取消", "退出", "cancel", "quit", "exit"}


@register(PLUGIN_NAME, "Lingyu", "回声洞插件", PLUGIN_VERSION)
class EchoCavePlugin(Star):
    def __init__(self, context: Context, config: AstrBotConfig | None = None):
        super().__init__(context)
        self.config = config or {}
        self._data_path = Path(__file__).resolve().parent / DATA_FILE_NAME
        self._lock = asyncio.Lock()

    async def initialize(self):
        """插件初始化时确保数据文件存在。"""
        try:
            async with self._lock:
                store = self._read_store_unlocked()
                self._write_store_unlocked(store)
        except Exception:
            logger.exception("初始化回声洞数据文件失败")

    @filter.command("回声洞")
    async def submit_echo(self, event: AstrMessageEvent):
        """投稿一条文本、图片或图文混合回声。"""
        async for result in self._submit_command_flow(event, strip_command=True):
            yield result
        event.stop_event()

    @filter.command("cavepost")
    async def submit_echo_english(self, event: AstrMessageEvent):
        """使用 cavepost 命令投稿一条回声。"""
        async for result in self._submit_command_flow(event, strip_command=True):
            yield result
        event.stop_event()

    @filter.command("听回声")
    async def listen_echo(self, event: AstrMessageEvent):
        """随机听一条历史投稿。"""
        result = await self._create_random_echo_result(event)
        yield result
        event.stop_event()

    @filter.command("cave")
    async def listen_echo_cave(self, event: AstrMessageEvent):
        """使用 cave 命令执行回声洞操作。"""
        async for result in self._dispatch_cave_command_flow(event):
            yield result
        event.stop_event()

    async def _submit_command_flow(self, event: AstrMessageEvent, strip_command: bool):
        try:
            submission = await self._parse_submission(event, strip_command=strip_command)
            if submission is not None:
                yield await self._save_submission_result(event, submission)
                return

            yield event.plain_result(
                f"已进入回声洞投稿模式。请在 {SUBMIT_TIMEOUT_SECONDS} 秒内发送文本、图片或图文内容；"
                "回复别人的消息会把那条消息一起加入投稿。发送“取消”可退出。"
            )
            try:
                await self._wait_for_submission(event)
            except TimeoutError:
                yield event.plain_result("回声洞投稿已超时，请重新发送投稿指令。")
        except Exception:
            logger.exception("处理回声洞投稿失败")
            yield event.plain_result("投稿保存失败，请稍后重试。")

    async def _dispatch_cave_command_flow(self, event: AstrMessageEvent):
        parsed = self._parse_cave_cli(event)
        mode = parsed["mode"]

        if mode == "add":
            async for result in self._submit_command_flow(event, strip_command=True):
                yield result
            return

        if mode == "help":
            yield event.plain_result(self._help_text())
            return

        if mode == "list":
            yield await self._create_my_list_result(event)
            return

        if mode == "delete":
            yield await self._create_delete_result(event, parsed.get("value", ""))
            return

        if mode == "get":
            yield await self._create_get_result(event, parsed.get("value", ""))
            return

        if mode == "all":
            yield await self._create_list_result(event)
            return

        if mode == "listen":
            yield await self._create_random_echo_result(event)
            return

        yield event.plain_result(
            "未知参数。使用 `.cave -h` 查看帮助。"
        )

    async def _create_random_echo_result(self, event: AstrMessageEvent):
        try:
            entry = await self._pick_random_entry()
            if entry is None:
                return event.plain_result("回声洞里还没有任何投稿。")

            chain, failed_images = self._build_entry_chain(entry)
            if not chain:
                return event.plain_result(
                    f"抽到了编号 #{entry.get('id', '?')} 的投稿，但图片资源已不可用。"
                )

            if failed_images:
                note = f"\n（其中 {failed_images} 张图片因资源不可用而未能发送）"
                if chain and self._is_plain_component(chain[0]):
                    self._append_text_to_plain(chain[0], note)
                else:
                    chain.append(Comp.Plain(note.strip()))

            return event.chain_result(chain)
        except Exception:
            logger.exception("随机发送回声失败")
            return event.plain_result("发送失败，请稍后重试。")

    @filter.command("回声洞列表")
    async def list_echoes(self, event: AstrMessageEvent):
        """管理员查看最近 20 条投稿。"""
        result = await self._create_list_result(event)
        yield result
        event.stop_event()

    @filter.command("cavelist")
    async def list_my_echoes(self, event: AstrMessageEvent):
        """查看当前用户自己的投稿记录。"""
        result = await self._create_my_list_result(event)
        yield result
        event.stop_event()

    async def _create_list_result(self, event: AstrMessageEvent):
        admin_error = self._require_admin_result(event)
        if admin_error is not None:
            return admin_error

        try:
            entries = await self._get_recent_entries(LIST_LIMIT)
            if not entries:
                return event.plain_result("回声洞里还没有任何投稿。")

            lines = ["最近 20 条投稿："]
            for entry in entries:
                lines.append(
                    (
                        f"#{entry.get('id', '?')} | 类型：{self._display_type(entry.get('type'))} | "
                        f"摘要：{self._summarize_text(self._entry_summary_text(entry))} | "
                        f"图片：{self._entry_image_count(entry)} 张 | "
                        f"时间：{entry.get('created_at', '-')}"
                    )
                )

            return event.plain_result("\n".join(lines))
        except Exception:
            logger.exception("查看回声洞列表失败")
            return event.plain_result("读取列表失败，请稍后重试。")

    async def _create_my_list_result(self, event: AstrMessageEvent):
        submitter = self._build_submitter_info(event)
        lookup_key = submitter.get("lookup_key", "")
        if not lookup_key:
            return event.plain_result("当前无法识别你的投稿身份，请稍后再试。")

        try:
            entries = await self._get_entries_by_submitter(lookup_key, LIST_LIMIT)
            if not entries:
                return event.plain_result("你当前还没有投稿到回声洞。")

            lines = [f"你的回声洞投稿（最近 {len(entries)} 条）："]
            for entry in entries:
                lines.append(
                    (
                        f"#{entry.get('id', '?')} | 类型：{self._display_type(entry.get('type'))} | "
                        f"摘要：{self._summarize_text(self._entry_summary_text(entry))} | "
                        f"图片：{self._entry_image_count(entry)} 张 | "
                        f"时间：{entry.get('created_at', '-')}"
                    )
                )

            return event.plain_result("\n".join(lines))
        except Exception:
            logger.exception("查看个人回声洞列表失败")
            return event.plain_result("读取你的投稿列表失败，请稍后重试。")

    @filter.command("删除回声")
    async def delete_echo(self, event: AstrMessageEvent, entry_id: str = ""):
        """管理员删除指定编号的投稿。"""
        result = await self._create_delete_result(event, entry_id)
        yield result
        event.stop_event()

    @filter.command("cavedel")
    async def delete_echo_english(self, event: AstrMessageEvent, entry_id: str = ""):
        """管理员使用 cavedel 删除指定编号的投稿。"""
        result = await self._create_delete_result(event, entry_id)
        yield result
        event.stop_event()

    async def _create_delete_result(self, event: AstrMessageEvent, entry_id: str = ""):
        admin_error = self._require_admin_result(event)
        if admin_error is not None:
            return admin_error

        normalized_entry_id = entry_id.strip()
        if not normalized_entry_id:
            return event.plain_result("用法：`.cavedel 编号`")

        if not normalized_entry_id.isdigit():
            return event.plain_result("编号必须是正整数。")

        try:
            deleted = await self._delete_entry(int(normalized_entry_id))
            if deleted is None:
                return event.plain_result(f"未找到编号 #{normalized_entry_id} 的投稿。")

            return event.plain_result(f"已删除编号 #{normalized_entry_id} 的投稿。")
        except Exception:
            logger.exception("删除回声失败")
            return event.plain_result("删除失败，请稍后重试。")

    async def _create_get_result(self, event: AstrMessageEvent, entry_id: str = ""):
        normalized_entry_id = entry_id.strip()
        if not normalized_entry_id:
            return event.plain_result("用法：`.cave -g 编号`")

        if not normalized_entry_id.isdigit():
            return event.plain_result("编号必须是正整数。")

        try:
            entry = await self._get_entry_by_id(int(normalized_entry_id))
            if entry is None:
                return event.plain_result(f"未找到编号 #{normalized_entry_id} 的回声。")

            chain, failed_images = self._build_entry_chain(entry)
            if not chain:
                return event.plain_result(
                    f"找到了编号 #{normalized_entry_id} 的回声，但图片资源已不可用。"
                )

            if failed_images:
                note = f"\n（其中 {failed_images} 张图片因资源不可用而未能发送）"
                if chain and self._is_plain_component(chain[0]):
                    self._append_text_to_plain(chain[0], note)
                else:
                    chain.append(Comp.Plain(note.strip()))

            return event.chain_result(chain)
        except Exception:
            logger.exception("按编号查看回声失败")
            return event.plain_result("读取失败，请稍后重试。")

    @filter.command("回声洞帮助")
    async def echo_help(self, event: AstrMessageEvent):
        """显示回声洞插件帮助信息。"""
        yield event.plain_result(self._help_text())
        event.stop_event()

    @filter.command("cavehelp")
    async def echo_help_english(self, event: AstrMessageEvent):
        """使用 cavehelp 显示回声洞插件帮助信息。"""
        yield event.plain_result(self._help_text())
        event.stop_event()

    async def _wait_for_submission(self, event: AstrMessageEvent) -> None:
        @session_waiter(timeout=SUBMIT_TIMEOUT_SECONDS, record_history_chains=False)
        async def cave_submit_waiter(controller: SessionController, follow_event: AstrMessageEvent):
            try:
                message_text = str(getattr(follow_event, "message_str", "") or "").strip()
                if message_text.lower() in CANCEL_SUBMIT_WORDS or message_text in CANCEL_SUBMIT_WORDS:
                    await follow_event.send(follow_event.plain_result("已取消本次回声洞投稿。"))
                    controller.stop()
                    return

                submission = await self._parse_submission(follow_event, strip_command=False)
                if submission is None:
                    await follow_event.send(
                        follow_event.plain_result(
                            "投稿内容为空。请发送文本、图片、图文内容，或回复一条消息来投稿。发送“取消”可退出。"
                        )
                    )
                    return

                await follow_event.send(await self._save_submission_result(follow_event, submission))
                controller.stop()
            except Exception:
                logger.exception("处理回声洞会话投稿失败")
                await follow_event.send(follow_event.plain_result("投稿保存失败，请稍后重试。"))
                controller.stop()
            finally:
                follow_event.stop_event()

        await cave_submit_waiter(event)

    async def _save_submission_result(self, event: AstrMessageEvent, submission: dict[str, Any]):
        entry_id = await self._append_entry(submission)
        return event.plain_result(f"投稿已收录，编号 #{entry_id}。")

    def _parse_cave_cli(self, event: AstrMessageEvent) -> dict[str, str]:
        args_text = self._extract_cave_argument_text(event)
        if not args_text:
            return {"mode": "listen", "value": ""}

        parts = args_text.split(maxsplit=1)
        option = parts[0].lower()
        remainder = parts[1].strip() if len(parts) > 1 else ""

        if option in {"-a", "--add", "--post"}:
            return {"mode": "add", "value": remainder}
        if option in {"-h", "--help"}:
            return {"mode": "help", "value": ""}
        if option in {"-l", "--list"}:
            return {"mode": "list", "value": ""}
        if option in {"-d", "--delete"}:
            return {"mode": "delete", "value": remainder}
        if option in {"-g", "--get"}:
            return {"mode": "get", "value": remainder}
        if option in {"--all", "-all"}:
            return {"mode": "all", "value": ""}

        return {"mode": "unknown", "value": args_text}

    def _extract_cave_argument_text(self, event: AstrMessageEvent) -> str:
        message_str = str(getattr(event, "message_str", "") or "").strip()
        if message_str:
            stripped = re.sub(
                r"^\s*[!/.／。]?(?i:cave|听回声)(?:\s+|$)",
                "",
                message_str,
                count=1,
            ).strip()
            if stripped != message_str:
                return stripped
            if message_str.startswith("-"):
                return message_str

        for segment in list(event.get_messages() or []):
            if not self._is_text_segment(segment):
                continue
            text = self._extract_segment_text(segment).strip()
            if not text:
                continue
            stripped = re.sub(
                r"^\s*[!/.／。]?(?i:cave|听回声)(?:\s+|$)",
                "",
                text,
                count=1,
            ).strip()
            if stripped != text:
                return stripped
            if text.startswith("-"):
                return text
        return ""

    async def _append_entry(self, submission: dict[str, Any]) -> int:
        async with self._lock:
            store = self._read_store_unlocked()
            entry_id = int(store["next_id"])
            entry = {
                "id": entry_id,
                "type": submission["type"],
                "text": submission["text"],
                "images": submission["images"],
                "quote": submission.get("quote"),
                "created_at": submission["created_at"],
                "submitter": submission["submitter"],
            }
            store["entries"].append(entry)
            store["next_id"] = entry_id + 1
            self._write_store_unlocked(store)
            return entry_id

    async def _pick_random_entry(self) -> dict[str, Any] | None:
        async with self._lock:
            store = self._read_store_unlocked()
            entries = store.get("entries", [])
            if not entries:
                return None
            return deepcopy(random.choice(entries))

    async def _get_recent_entries(self, limit: int) -> list[dict[str, Any]]:
        async with self._lock:
            store = self._read_store_unlocked()
            entries = list(store.get("entries", []))
            recent_entries = entries[-limit:]
            recent_entries.reverse()
            return deepcopy(recent_entries)

    async def _get_entries_by_submitter(
        self,
        lookup_key: str,
        limit: int,
    ) -> list[dict[str, Any]]:
        async with self._lock:
            store = self._read_store_unlocked()
            matched_entries = [
                entry
                for entry in store.get("entries", [])
                if self._extract_submitter_lookup_key(entry) == lookup_key
            ]
            matched_entries = matched_entries[-limit:]
            matched_entries.reverse()
            return deepcopy(matched_entries)

    async def _get_entry_by_id(self, entry_id: int) -> dict[str, Any] | None:
        async with self._lock:
            store = self._read_store_unlocked()
            for entry in store.get("entries", []):
                try:
                    if int(entry.get("id", -1)) == entry_id:
                        return deepcopy(entry)
                except (TypeError, ValueError):
                    continue
            return None

    async def _delete_entry(self, entry_id: int) -> dict[str, Any] | None:
        async with self._lock:
            store = self._read_store_unlocked()
            entries = store.get("entries", [])
            for index, entry in enumerate(entries):
                if int(entry.get("id", -1)) == entry_id:
                    deleted = entries.pop(index)
                    self._write_store_unlocked(store)
                    return deepcopy(deleted)
            return None

    def _extract_content_from_segments(
        self,
        segments: list[Any],
        strip_command: bool,
        fallback_text: Any = "",
    ) -> dict[str, Any]:
        text_parts: list[str] = []
        images: list[dict[str, Any]] = []
        command_stripped = not strip_command

        for segment in segments:
            if self._is_reply_segment(segment):
                continue

            if self._is_image_segment(segment):
                image_payload = self._serialize_image_segment(segment)
                if image_payload is not None:
                    images.append(image_payload)
                continue

            if self._is_text_segment(segment):
                text_value = self._extract_segment_text(segment)
                if not command_stripped:
                    text_value = self._strip_submit_command(text_value)
                    command_stripped = True

                cleaned_text = text_value.strip()
                if cleaned_text:
                    text_parts.append(cleaned_text)

        if not text_parts:
            normalized_fallback = str(fallback_text or "")
            if strip_command:
                normalized_fallback = self._strip_submit_command(normalized_fallback)
            normalized_fallback = normalized_fallback.strip()
            if normalized_fallback:
                text_parts.append(normalized_fallback)

        return {"text": "\n".join(text_parts).strip(), "images": images}

    async def _resolve_reply_quote(
        self,
        event: AstrMessageEvent,
        message_chain: list[Any],
    ) -> dict[str, Any] | None:
        reply_ref = self._extract_reply_reference(event, message_chain)
        if reply_ref is None:
            return None

        embedded_payload = self._extract_embedded_reply_payload(event)
        if embedded_payload is not None:
            quote = self._normalize_quote_payload(
                embedded_payload,
                default_message_id=reply_ref.get("message_id"),
            )
            if quote is not None:
                return quote

        fetched_payload = await self._fetch_reply_payload(event, reply_ref)
        if fetched_payload is not None:
            quote = self._normalize_quote_payload(
                fetched_payload,
                default_message_id=reply_ref.get("message_id"),
            )
            if quote is not None:
                return quote

        return None

    def _extract_reply_reference(
        self,
        event: AstrMessageEvent,
        message_chain: list[Any],
    ) -> dict[str, Any] | None:
        for segment in message_chain:
            ref = self._extract_reply_reference_from_segment(segment)
            if ref is not None:
                return ref

        for mapping in self._raw_event_mappings(event):
            ref = self._extract_reply_reference_from_mapping(mapping)
            if ref is not None:
                return ref
        return None

    def _extract_reply_reference_from_segment(self, segment: Any) -> dict[str, Any] | None:
        if not self._is_reply_segment(segment):
            return None

        payload = self._extract_segment_payload(segment)
        message_id = self._first_string(
            self._safe_string(payload.get("id")) if isinstance(payload, dict) else None,
            self._safe_string(payload.get("message_id")) if isinstance(payload, dict) else None,
            self._safe_string(payload.get("mid")) if isinstance(payload, dict) else None,
        )
        if message_id:
            return {"message_id": message_id}
        return None

    def _extract_reply_reference_from_mapping(self, mapping: dict[str, Any]) -> dict[str, Any] | None:
        for key in ("reply", "quote", "source", "quoted_message", "referenced_message"):
            value = mapping.get(key)
            if isinstance(value, dict):
                message_id = self._first_string(
                    self._safe_string(value.get("id")),
                    self._safe_string(value.get("message_id")),
                    self._safe_string(value.get("mid")),
                )
                if message_id:
                    return {"message_id": message_id}

        message = mapping.get("message")
        if isinstance(message, list):
            for segment in message:
                ref = self._extract_reply_reference_from_segment(segment)
                if ref is not None:
                    return ref
        return None

    def _extract_embedded_reply_payload(self, event: AstrMessageEvent) -> Any:
        for mapping in self._raw_event_mappings(event):
            for key in ("reply", "quote", "quoted_message", "referenced_message"):
                value = mapping.get(key)
                if value is not None:
                    return value
        return None

    def _raw_event_mappings(self, event: AstrMessageEvent) -> list[dict[str, Any]]:
        candidates = [
            getattr(event, "raw_message", None),
            getattr(getattr(event, "message_obj", None), "raw_message", None),
            getattr(event, "message_obj", None),
        ]
        mappings: list[dict[str, Any]] = []
        for candidate in candidates:
            serialized = self._to_serializable(candidate)
            if isinstance(serialized, dict):
                mappings.append(serialized)
        return mappings

    async def _fetch_reply_payload(self, event: AstrMessageEvent, reply_ref: dict[str, Any]) -> Any:
        message_id = self._safe_string(reply_ref.get("message_id"))
        if not message_id:
            return None

        bot = getattr(event, "bot", None)
        api = getattr(bot, "api", None) if bot is not None else None
        call_action = getattr(api, "call_action", None)
        if not callable(call_action):
            return None

        try:
            result = await call_action("get_msg", message_id=message_id)
        except Exception:
            logger.exception("获取被回复消息失败")
            return None
        return self._unwrap_api_response(result)

    def _unwrap_api_response(self, payload: Any) -> Any:
        serialized = self._to_serializable(payload)
        if not isinstance(serialized, dict):
            return serialized
        if isinstance(serialized.get("data"), dict):
            return serialized["data"]
        return serialized

    def _normalize_quote_payload(
        self,
        payload: Any,
        default_message_id: str | None = None,
    ) -> dict[str, Any] | None:
        serialized = self._to_serializable(payload)
        message_id = default_message_id

        if isinstance(serialized, list):
            content = self._extract_content_from_segments(serialized, strip_command=False)
        elif isinstance(serialized, dict):
            message_id = self._first_string(
                self._safe_string(serialized.get("message_id")),
                self._safe_string(serialized.get("id")),
                default_message_id,
            )
            segment_source = serialized.get("message")
            if not isinstance(segment_source, list):
                segment_source = serialized.get("content")
            if not isinstance(segment_source, list):
                segment_source = serialized.get("message_chain")

            if isinstance(segment_source, list):
                content = self._extract_content_from_segments(segment_source, strip_command=False)
            else:
                text = self._first_string(
                    serialized.get("message_str"),
                    serialized.get("raw_message"),
                    serialized.get("text"),
                    serialized.get("content") if isinstance(serialized.get("content"), str) else None,
                ) or ""
                content = {"text": text.strip(), "images": []}
        else:
            return None

        text = str(content.get("text", "") or "").strip()
        images = self._normalize_images(content.get("images"))
        if not text and not images:
            return None

        quote: dict[str, Any] = {"text": text, "images": images}
        if message_id:
            quote["message_id"] = message_id
        return quote

    async def _parse_submission(
        self,
        event: AstrMessageEvent,
        strip_command: bool,
    ) -> dict[str, Any] | None:
        message_chain = list(event.get_messages() or [])
        content = self._extract_content_from_segments(
            message_chain,
            strip_command=strip_command,
            fallback_text=getattr(event, "message_str", ""),
        )
        quote = await self._resolve_reply_quote(event, message_chain)
        text = content["text"]
        images = content["images"]
        quote_text = self._quote_text(quote)
        quote_images = self._quote_images(quote)

        has_text = bool(text or quote_text)
        has_images = bool(images or quote_images)
        if has_text and has_images:
            submit_type = "mixed"
        elif has_images:
            submit_type = "image"
        elif has_text:
            submit_type = "text"
        else:
            return None

        return {
            "type": submit_type,
            "text": text,
            "images": images,
            "quote": quote,
            "created_at": datetime.now().astimezone().strftime("%Y-%m-%d %H:%M:%S %z"),
            "submitter": self._build_submitter_info(event),
        }

    def _build_entry_chain(self, entry: dict[str, Any]) -> tuple[list[Any], int]:
        chain: list[Any] = []
        failed_images = 0
        text = str(entry.get("text", "") or "").strip()
        images = self._normalize_images(entry.get("images"))
        quote = self._normalize_quote(entry.get("quote"))
        quote_text = self._quote_text(quote)
        quote_images = self._quote_images(quote)
        header_text = self._format_entry_header(entry)
        footer_text = self._format_entry_footer(entry)

        if header_text:
            chain.append(Comp.Plain(header_text))

        if quote_text:
            chain.append(Comp.Plain(f"回复：\n{quote_text}"))
        elif quote_images:
            chain.append(Comp.Plain("回复：[图片]"))

        for image_info in quote_images:
            component = self._build_image_component(image_info)
            if component is None:
                failed_images += 1
                continue
            chain.append(component)

        if text:
            chain.append(Comp.Plain(text))

        for image_info in images:
            component = self._build_image_component(image_info)
            if component is None:
                failed_images += 1
                continue
            chain.append(component)

        if footer_text:
            chain.append(Comp.Plain(footer_text))

        return chain, failed_images

    def _build_image_component(self, image_info: dict[str, Any]) -> Any | None:
        resend = image_info.get("resend")
        if isinstance(resend, dict):
            resend_type = str(resend.get("type", "")).lower()
            resend_value = resend.get("value")
            if resend_type == "url" and isinstance(resend_value, str) and resend_value:
                return Comp.Image.fromURL(resend_value)
            if resend_type == "file" and isinstance(resend_value, str) and resend_value:
                return Comp.Image.fromFileSystem(resend_value)

        url = self._first_string(
            image_info.get("url"),
            image_info.get("image_url"),
            self._extract_string_from_mapping(image_info.get("segment_data"), ("url", "image_url", "src")),
        )
        if url:
            return Comp.Image.fromURL(url)

        file_path = self._first_string(
            image_info.get("file_path"),
            image_info.get("file"),
            image_info.get("path"),
            self._extract_string_from_mapping(
                image_info.get("segment_data"),
                ("file", "path", "file_path", "filepath"),
            ),
        )
        if file_path:
            return Comp.Image.fromFileSystem(file_path)

        return None

    def _serialize_image_segment(self, segment: Any) -> dict[str, Any] | None:
        raw_payload = self._extract_segment_payload(segment)
        serializable_payload = self._to_serializable(raw_payload)
        url = self._first_string(
            getattr(segment, "url", None),
            getattr(segment, "image_url", None),
            self._extract_string_from_mapping(serializable_payload, ("url", "image_url", "src")),
        )
        file_path = self._first_string(
            getattr(segment, "file", None),
            getattr(segment, "path", None),
            getattr(segment, "file_path", None),
            self._extract_string_from_mapping(
                serializable_payload,
                ("file", "path", "file_path", "filepath"),
            ),
        )

        image_info: dict[str, Any] = {
            "segment_type": self._segment_type_name(segment),
            "segment_data": serializable_payload,
        }
        if url:
            image_info["url"] = url
            image_info["resend"] = {"type": "url", "value": url}
        elif file_path:
            image_info["file_path"] = file_path
            image_info["resend"] = {"type": "file", "value": file_path}

        if not serializable_payload and "resend" not in image_info:
            segment_repr = repr(segment)
            if segment_repr:
                image_info["repr"] = segment_repr

        if (
            image_info.get("segment_data")
            or image_info.get("url")
            or image_info.get("file_path")
            or image_info.get("repr")
        ):
            return image_info
        return None

    def _read_store_unlocked(self) -> dict[str, Any]:
        default_store = {"next_id": 1, "entries": []}
        if not self._data_path.exists():
            return default_store

        try:
            raw_text = self._data_path.read_text(encoding="utf-8")
            data = json.loads(raw_text)
        except json.JSONDecodeError:
            backup_path = self._make_broken_backup()
            logger.warning(f"回声洞数据文件损坏，已备份到 {backup_path}")
            return default_store
        except OSError:
            logger.exception("读取回声洞数据文件失败")
            return default_store

        if not isinstance(data, dict):
            logger.warning("回声洞数据文件格式错误，将重置为默认结构")
            return default_store

        entries = data.get("entries", [])
        next_id = data.get("next_id", 1)
        if not isinstance(entries, list):
            logger.warning("回声洞 entries 字段格式错误，将重置为空列表")
            entries = []

        if not isinstance(next_id, int) or next_id < 1:
            next_id = self._infer_next_id(entries)

        normalized_entries = [entry for entry in entries if isinstance(entry, dict)]
        next_id = max(next_id, self._infer_next_id(normalized_entries))
        return {"next_id": next_id, "entries": normalized_entries}

    def _write_store_unlocked(self, store: dict[str, Any]) -> None:
        self._data_path.parent.mkdir(parents=True, exist_ok=True)
        temp_path = self._data_path.with_name(f"{self._data_path.name}.tmp")
        payload = json.dumps(store, ensure_ascii=False, indent=2)
        temp_path.write_text(payload, encoding="utf-8")
        temp_path.replace(self._data_path)

    def _make_broken_backup(self) -> Path:
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        backup_path = self._data_path.with_name(f"{self._data_path.stem}.broken.{timestamp}.json")
        try:
            shutil.copy2(self._data_path, backup_path)
        except OSError:
            logger.exception("备份损坏的回声洞数据文件失败")
        return backup_path

    def _infer_next_id(self, entries: list[dict[str, Any]]) -> int:
        max_id = 0
        for entry in entries:
            try:
                max_id = max(max_id, int(entry.get("id", 0)))
            except (TypeError, ValueError):
                continue
        return max_id + 1

    def _normalize_images(self, images: Any) -> list[dict[str, Any]]:
        if not isinstance(images, list):
            return []
        return [image for image in images if isinstance(image, dict)]

    def _normalize_quote(self, quote: Any) -> dict[str, Any]:
        if isinstance(quote, dict):
            return quote
        return {}

    def _quote_text(self, quote: Any) -> str:
        normalized = self._normalize_quote(quote)
        return str(normalized.get("text", "") or "").strip()

    def _quote_images(self, quote: Any) -> list[dict[str, Any]]:
        normalized = self._normalize_quote(quote)
        return self._normalize_images(normalized.get("images"))

    def _entry_summary_text(self, entry: dict[str, Any]) -> str:
        parts: list[str] = []
        quote_text = self._quote_text(entry.get("quote"))
        text = str(entry.get("text", "") or "").strip()
        if quote_text:
            parts.append(f"回复：{quote_text}")
        if text:
            parts.append(text)
        return "\n".join(parts).strip()

    def _entry_image_count(self, entry: dict[str, Any]) -> int:
        return len(self._normalize_images(entry.get("images"))) + len(self._quote_images(entry.get("quote")))

    def _display_type(self, submit_type: Any) -> str:
        mapping = {
            "text": "纯文本",
            "image": "纯图片",
            "mixed": "图文混合",
        }
        return mapping.get(str(submit_type), str(submit_type or "未知"))

    def _summarize_text(self, text: Any) -> str:
        normalized = re.sub(r"\s+", " ", str(text or "")).strip()
        if not normalized:
            return "无文本"
        if len(normalized) <= SUMMARY_LIMIT:
            return normalized
        return normalized[: SUMMARY_LIMIT - 1] + "…"

    def _help_text(self) -> str:
        return "\n".join(
            [
                "回声洞命令：",
                ".cave                随机听一条回声",
                ".cave -a [内容]      投稿；不带内容则进入 60 秒投稿模式",
                ".cave -g 编号        查看指定编号的回声",
                ".cave -l             查看本人投稿",
                ".cave -d 编号        删除投稿（管理员）",
                ".cave --all          查看全局投稿（管理员）",
                ".cave -h             查看帮助",
                "",
                f"发送投稿指令后，可在 {SUBMIT_TIMEOUT_SECONDS} 秒内继续投稿。",
                "回复别人的消息进行投稿时，会把那条消息一并收入回声洞。",
                "投稿支持：纯文本、纯图片、图文混合。",
                "回声会保存脱敏署名，并在 .cave 时显示昵称和脱敏 ID。",
                "插件设置支持追加管理员账号列表。",
                "",
                "兼容旧命令：.cavepost、.cavehelp、.cavelist、.cavedel、/回声洞、/听回声、/回声洞帮助、/回声洞列表、/删除回声 编号。",
            ]
        )

    def _is_text_segment(self, segment: Any) -> bool:
        segment_type = self._segment_type_name(segment)
        return segment_type in {"plain", "text"}

    def _is_image_segment(self, segment: Any) -> bool:
        segment_type = self._segment_type_name(segment)
        return segment_type == "image"

    def _is_reply_segment(self, segment: Any) -> bool:
        segment_type = self._segment_type_name(segment)
        return segment_type in {"reply", "quote", "reference"}

    def _segment_type_name(self, segment: Any) -> str:
        if isinstance(segment, dict):
            raw_type = segment.get("type")
            if raw_type is not None:
                return str(raw_type).split(".")[-1].lower()
        explicit_type = getattr(segment, "type", None)
        if explicit_type is not None:
            return str(explicit_type).split(".")[-1].lower()
        return type(segment).__name__.lower()

    def _extract_segment_text(self, segment: Any) -> str:
        for attr_name in ("text", "plain", "content", "message", "value"):
            value = getattr(segment, attr_name, None)
            if isinstance(value, str):
                return value

        payload = self._extract_segment_payload(segment)
        if isinstance(payload, dict):
            for key in ("text", "plain", "content", "message", "value"):
                value = payload.get(key)
                if isinstance(value, str):
                    return value

            for value in payload.values():
                if isinstance(value, str):
                    return value
        return ""

    def _extract_segment_payload(self, segment: Any) -> dict[str, Any]:
        if isinstance(segment, dict):
            payload = segment.get("data")
            if isinstance(payload, dict):
                return payload
            return segment

        for method_name in ("model_dump", "dict"):
            method = getattr(segment, method_name, None)
            if callable(method):
                try:
                    payload = method()
                    if isinstance(payload, dict):
                        return payload
                except Exception:
                    continue

        if hasattr(segment, "__dict__"):
            return {
                key: value
                for key, value in vars(segment).items()
                if not key.startswith("_") and not callable(value)
            }
        return {}

    def _to_serializable(self, value: Any, depth: int = 0) -> Any:
        if depth > 5:
            return repr(value)
        if value is None or isinstance(value, (str, int, float, bool)):
            return value
        if isinstance(value, Path):
            return str(value)
        if isinstance(value, dict):
            return {
                str(key): self._to_serializable(item, depth + 1)
                for key, item in value.items()
            }
        if isinstance(value, (list, tuple, set)):
            return [self._to_serializable(item, depth + 1) for item in value]

        for method_name in ("model_dump", "dict"):
            method = getattr(value, method_name, None)
            if callable(method):
                try:
                    dumped = method()
                    return self._to_serializable(dumped, depth + 1)
                except Exception:
                    continue

        if hasattr(value, "__dict__"):
            return self._to_serializable(
                {
                    key: item
                    for key, item in vars(value).items()
                    if not key.startswith("_") and not callable(item)
                },
                depth + 1,
            )

        return repr(value)

    def _strip_submit_command(self, text: Any) -> str:
        normalized = str(text or "")
        pattern = (
            r"^\s*(?:[!/.／。]?(?:回声洞|(?i:cavepost|cave))(?:\s+|$))?"
            r"(?:(?:-a|--add|--post)\b\s*)?"
        )
        return re.sub(pattern, "", normalized, count=1).strip()

    def _extract_string_from_mapping(self, mapping: Any, keys: tuple[str, ...]) -> str | None:
        if not isinstance(mapping, dict):
            return None
        for key in keys:
            value = mapping.get(key)
            if isinstance(value, str) and value:
                return value
        return None

    def _first_string(self, *values: Any) -> str | None:
        for value in values:
            if isinstance(value, str) and value:
                return value
        return None

    def _is_plain_component(self, component: Any) -> bool:
        component_type = getattr(component, "type", None)
        if component_type is not None:
            return str(component_type).split(".")[-1].lower() == "plain"
        return type(component).__name__.lower() == "plain"

    def _append_text_to_plain(self, component: Any, extra_text: str) -> None:
        for attr_name in ("text", "plain", "content", "message", "value"):
            value = getattr(component, attr_name, None)
            if isinstance(value, str):
                setattr(component, attr_name, value + extra_text)
                return

    def _format_entry_header(self, entry: dict[str, Any]) -> str:
        entry_id = entry.get("id", "?")
        return f"回声洞 —— ({entry_id})"

    def _format_entry_footer(self, entry: dict[str, Any]) -> str:
        submitter = self._normalize_submitter(entry.get("submitter"))
        display = str(submitter.get("display", "") or "").strip()
        if not display:
            return ""
        return f"—— {display}"

    def _build_submitter_info(self, event: AstrMessageEvent) -> dict[str, str]:
        sender_name = self._normalize_sender_name(self._call_event_getter(event, "get_sender_name"))
        sender_id = self._normalize_sender_id(self._call_event_getter(event, "get_sender_id"))
        platform_name = self._get_platform_name(event)
        identity_source = f"{platform_name}:{sender_id or sender_name}"
        lookup_key = hashlib.sha256(identity_source.encode("utf-8")).hexdigest()
        masked_id = self._mask_sender_id(sender_id)

        display = sender_name
        if masked_id:
            display = f"{sender_name}--{masked_id}"

        return {
            "lookup_key": lookup_key,
            "name": sender_name,
            "masked_id": masked_id,
            "display": display,
        }

    def _extract_submitter_lookup_key(self, entry: dict[str, Any]) -> str:
        submitter = self._normalize_submitter(entry.get("submitter"))
        lookup_key = submitter.get("lookup_key")
        if isinstance(lookup_key, str):
            return lookup_key
        return ""

    def _normalize_submitter(self, submitter: Any) -> dict[str, Any]:
        if isinstance(submitter, dict):
            return submitter
        return {}

    def _normalize_sender_name(self, sender_name: Any) -> str:
        normalized = re.sub(r"\s+", " ", str(sender_name or "")).strip()
        if not normalized:
            return "回声用户"
        return normalized[:32]

    def _normalize_sender_id(self, sender_id: Any) -> str:
        return re.sub(r"\s+", "", str(sender_id or "")).strip()

    def _mask_sender_id(self, sender_id: str) -> str:
        if not sender_id:
            return ""
        if len(sender_id) <= 3:
            return "*" * len(sender_id)
        if len(sender_id) <= 6:
            return sender_id[0] + ("*" * (len(sender_id) - 2)) + sender_id[-1]
        return f"{sender_id[:3]}***{sender_id[-3:]}"

    def _get_platform_name(self, event: AstrMessageEvent) -> str:
        getter = getattr(event, "get_platform_name", None)
        if callable(getter):
            try:
                return str(getter() or "")
            except Exception:
                return ""
        return ""

    def _call_event_getter(self, event: AstrMessageEvent, getter_name: str) -> Any:
        getter = getattr(event, getter_name, None)
        if callable(getter):
            try:
                return getter()
            except Exception:
                return ""
        return ""

    def _safe_string(self, value: Any) -> str | None:
        if value is None:
            return None
        string_value = str(value).strip()
        if not string_value:
            return None
        return string_value

    def _require_admin_result(self, event: AstrMessageEvent):
        if self._is_admin_user(event):
            return None
        return event.plain_result(
            "你没有回声洞管理员权限。请在插件设置的“管理员账号列表”中添加你的用户 ID。"
        )

    def _is_admin_user(self, event: AstrMessageEvent) -> bool:
        sender_id = self._normalize_sender_id(self._call_event_getter(event, "get_sender_id"))
        if sender_id and sender_id in self._configured_admin_ids():
            return True
        if self._extract_admin_like_flag(event):
            return True
        return False

    def _configured_admin_ids(self) -> set[str]:
        raw_value = self.config.get("admin_ids", "")
        if isinstance(raw_value, list):
            values = [str(item or "").strip() for item in raw_value]
        else:
            text = str(raw_value or "")
            values = [item.strip() for item in re.split(r"[\s,;，；]+", text)]
        return {value for value in values if value}

    def _extract_admin_like_flag(self, event: AstrMessageEvent) -> bool:
        direct_flag = self._call_event_getter(event, "is_admin")
        if isinstance(direct_flag, bool) and direct_flag:
            return True

        direct_owner = self._call_event_getter(event, "is_owner")
        if isinstance(direct_owner, bool) and direct_owner:
            return True

        candidates = [
            getattr(event, "message_obj", None),
            getattr(getattr(event, "message_obj", None), "sender", None),
            getattr(getattr(event, "message_obj", None), "raw_message", None),
            getattr(event, "raw_message", None),
        ]
        for candidate in candidates:
            if self._object_has_admin_like_flag(candidate):
                return True
        return False

    def _object_has_admin_like_flag(self, obj: Any) -> bool:
        if obj is None:
            return False
        if isinstance(obj, dict):
            return self._mapping_has_admin_like_flag(obj)

        for attr_name in ("is_admin", "admin", "is_owner", "owner"):
            value = getattr(obj, attr_name, None)
            if isinstance(value, bool) and value:
                return True

        for attr_name in ("role", "permission", "user_role"):
            if self._is_admin_like_role(getattr(obj, attr_name, None)):
                return True

        if hasattr(obj, "__dict__"):
            return self._mapping_has_admin_like_flag(vars(obj))
        return False

    def _mapping_has_admin_like_flag(self, mapping: dict[str, Any]) -> bool:
        for key in ("is_admin", "admin", "is_owner", "owner"):
            value = mapping.get(key)
            if isinstance(value, bool) and value:
                return True

        for key in ("role", "permission", "user_role"):
            if self._is_admin_like_role(mapping.get(key)):
                return True
        return False

    def _is_admin_like_role(self, value: Any) -> bool:
        if value is None:
            return False
        return str(value).strip().lower() in ADMIN_ROLE_KEYWORDS
