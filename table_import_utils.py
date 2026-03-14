#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

from collections.abc import Iterable, Sequence

IMPORT_TARGET_PURPLE = "#7c3aed"


def column_name_from_identifier(columns: Sequence[str], column_id: str) -> str:
    if column_id in columns:
        return str(column_id)
    if not column_id.startswith("#"):
        return ""
    try:
        idx = int(column_id[1:]) - 1
    except Exception:
        return ""
    if 0 <= idx < len(columns):
        return str(columns[idx])
    return ""


def parse_single_value_lines(
    lines: Iterable[str],
    label: str,
    *,
    allow_inner_whitespace: bool = False,
) -> list[str]:
    result: list[str] = []
    for i, line in enumerate(lines, start=1):
        value = str(line or "").strip()
        if not value or value.startswith("#"):
            continue
        if not allow_inner_whitespace and any(ch.isspace() for ch in value):
            raise RuntimeError(f"第 {i} 行{label}格式错误：不能包含空白字符")
        result.append(value)
    return result


def merge_column_values(
    drafts: list[dict[str, str]],
    fields: Sequence[str],
    target_field: str,
    values: Sequence[str],
) -> int:
    added_rows = 0
    for value in values:
        assigned = False
        for row in drafts:
            if not row.get(target_field, "").strip():
                row[target_field] = str(value).strip()
                assigned = True
                break
        if assigned:
            continue
        row = {field: "" for field in fields}
        row[target_field] = str(value).strip()
        drafts.append(row)
        added_rows += 1
    return added_rows


def heading_text(base_text: str, active: bool) -> str:
    if active:
        return f"{base_text} [列粘贴]"
    return base_text


def compute_column_highlight_geometry(
    tree,
    columns: Sequence[str],
    target: str,
) -> tuple[int, int] | None:
    if not target or target == "full" or target not in columns:
        return None
    if not hasattr(tree, "column"):
        return None

    widths: list[int] = []
    for column in columns:
        try:
            width = int(tree.column(column, "width"))
        except Exception:
            try:
                width = int((tree.column(column) or {}).get("width", 0))
            except Exception:
                return None
        widths.append(max(0, width))

    total_width = sum(widths)
    if total_width <= 0:
        return None

    try:
        viewport_width = int(tree.winfo_width())
    except Exception:
        viewport_width = 0
    if viewport_width <= 1:
        return None

    start_fraction = 0.0
    if hasattr(tree, "xview"):
        try:
            xview = tree.xview()
            if isinstance(xview, (tuple, list)) and xview:
                start_fraction = float(xview[0])
        except Exception:
            start_fraction = 0.0

    scroll_x = int(round(start_fraction * total_width))
    target_index = list(columns).index(target)
    left = sum(widths[:target_index]) - scroll_x
    width = widths[target_index]

    visible_left = max(0, left)
    visible_right = min(viewport_width, left + width)
    if visible_right <= visible_left:
        return None
    return visible_left, visible_right - visible_left


def update_import_target_bar(
    bar,
    tree,
    columns: Sequence[str],
    target: str,
    *,
    y: int = 1,
    height: int = 5,
) -> None:
    if bar is None:
        return
    geometry = compute_column_highlight_geometry(tree, columns, target)
    if geometry is None:
        try:
            bar.place_forget()
        except Exception:
            pass
        return

    x, width = geometry
    try:
        bar.place(x=x, y=y, width=width, height=height)
        if hasattr(bar, "lift"):
            bar.lift()
    except Exception:
        pass
