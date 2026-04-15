from __future__ import annotations

import math

from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup

OWNER_PANEL_CALLBACK = "owner:panel:Q4mN8s7A"
OWNER_PANEL_REFRESH_CALLBACK = "owner:panel:Q4mN8s7A:refresh"
OWNER_PANEL_HELP_CALLBACK = "owner:panel:Q4mN8s7A:help"


def main_menu_keyboard(*, show_accounts: bool, show_owner_panel: bool, show_profile: bool) -> InlineKeyboardMarkup:
    rows = [
        [
            InlineKeyboardButton(text="💬 Сообщения", callback_data="menu:messaging"),
            InlineKeyboardButton(text="🚀 Активность", callback_data="menu:engagement"),
        ],
    ]
    if show_accounts:
        rows.append(
            [
                InlineKeyboardButton(text="👥 Аккаунты", callback_data="menu:accounts"),
                InlineKeyboardButton(text="🗂️ Задачи", callback_data="menu:tasks"),
            ]
        )
    else:
        rows.append([InlineKeyboardButton(text="🗂️ Задачи", callback_data="menu:tasks")])
    if show_profile:
        rows.append([InlineKeyboardButton(text="👤 Профиль", callback_data="menu:profile")])
    if show_owner_panel:
        rows.append([InlineKeyboardButton(text="🔐 Админ-панель", callback_data=OWNER_PANEL_CALLBACK)])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def menu_section_keyboard(section: str) -> InlineKeyboardMarkup:
    if section == "messaging":
        rows = [
            [InlineKeyboardButton(text="📩 Рассылка", callback_data="menu:msg")],
            [InlineKeyboardButton(text="🤖 Команда боту", callback_data="menu:msgbot")],
            [InlineKeyboardButton(text="💥 В чат", callback_data="menu:msgchat")],
            [InlineKeyboardButton(text="📞 Звонки", callback_data="menu:call")],
        ]
    elif section == "engagement":
        rows = [
            [InlineKeyboardButton(text="➕ Вступление", callback_data="menu:join")],
            [InlineKeyboardButton(text="🚪 Выход", callback_data="menu:leave")],
            [InlineKeyboardButton(text="👍 Реакции", callback_data="menu:likep")],
            [InlineKeyboardButton(text="🗳 Голосование", callback_data="menu:vote")],
            [InlineKeyboardButton(text="🎁 Рефералы", callback_data="menu:refp")],
        ]
    else:
        rows = []

    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="menu:home")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def accounts_menu_keyboard(
    *,
    can_import_accounts: bool,
    can_export_accounts: bool,
    is_private_tenant: bool,
) -> InlineKeyboardMarkup:
    session_title = "📂 Мои сессии" if is_private_tenant else "📂 Сессии"
    rows = [[InlineKeyboardButton(text=session_title, callback_data="acc:session_manager")]]
    import_export_row: list[InlineKeyboardButton] = []
    if can_import_accounts:
        import_export_row.append(InlineKeyboardButton(text="📥 Импорт", callback_data="acc:import"))
    if can_export_accounts:
        import_export_row.append(InlineKeyboardButton(text="📤 Экспорт", callback_data="acc:export_menu"))
    if import_export_row:
        rows.append(import_export_row)
    rows.append([InlineKeyboardButton(text="📊 Статус", callback_data="acc:health")])
    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="menu:home")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def access_manager_keyboard(users: list[dict], *, owner_user_id: int) -> InlineKeyboardMarkup:
    rows = [
        [
            InlineKeyboardButton(text="👥 Пользователи", callback_data="admin_users"),
            InlineKeyboardButton(text="➕ Выдать доступ", callback_data="admin_grant_access"),
        ],
        [
            InlineKeyboardButton(text="🗂 Задачи", callback_data="admin_tasks"),
            InlineKeyboardButton(text="📊 Статистика", callback_data="admin_stats"),
        ],
        [InlineKeyboardButton(text="🛑 Стоп всё", callback_data="admin_stop_all")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="menu:home")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)


def access_users_keyboard(users: list[dict], *, page: int, total_pages: int) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    for item in users:
        text = f"@{item['username']}" if item.get("username") else f"id{item['telegram_id']}"
        rows.append([InlineKeyboardButton(text=text, callback_data=f"user:{item['telegram_id']}")])
    if total_pages > 1:
        nav_row = [
            InlineKeyboardButton(text="⬅️", callback_data=f"page:{max(1, page - 1)}"),
            InlineKeyboardButton(text=f"{page}/{total_pages}", callback_data=f"page:{page}"),
            InlineKeyboardButton(text="➡️", callback_data=f"page:{min(total_pages, page + 1)}"),
        ]
        rows.append(nav_row)
    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="back")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def access_user_actions_keyboard(*, telegram_id: int, is_blocked: bool) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(text="🎭 Сменить роль", callback_data=f"user_role:{telegram_id}")],
        [InlineKeyboardButton(text="💼 Сменить тариф", callback_data=f"user_tariff:{telegram_id}")],
        [InlineKeyboardButton(text="🚫 Разблокировать" if is_blocked else "🚫 Заблокировать", callback_data=f"user_block:{telegram_id}")],
        [InlineKeyboardButton(text="🗑 Удалить из БД", callback_data=f"user_delete:{telegram_id}")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="back")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)


def access_user_role_keyboard(*, telegram_id: int) -> InlineKeyboardMarkup:
    rows = [
        [
            InlineKeyboardButton(text="owner", callback_data=f"user_role_set:{telegram_id}:owner"),
            InlineKeyboardButton(text="admin", callback_data=f"user_role_set:{telegram_id}:admin"),
        ],
        [
            InlineKeyboardButton(text="internal", callback_data=f"user_role_set:{telegram_id}:internal"),
            InlineKeyboardButton(text="external", callback_data=f"user_role_set:{telegram_id}:external"),
        ],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="back")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)


def access_user_tariff_keyboard(*, telegram_id: int) -> InlineKeyboardMarkup:
    rows = [
        [
            InlineKeyboardButton(text="Trial", callback_data=f"user_tariff_set:{telegram_id}:trial"),
            InlineKeyboardButton(text="Standard", callback_data=f"user_tariff_set:{telegram_id}:standard"),
        ],
        [
            InlineKeyboardButton(text="Pro", callback_data=f"user_tariff_set:{telegram_id}:pro"),
            InlineKeyboardButton(text="Enterprise", callback_data=f"user_tariff_set:{telegram_id}:enterprise"),
        ],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="back")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)


def session_manager_keyboard(
    accounts: list[dict],
    *,
    can_add_accounts: bool,
    page: int,
    page_size: int,
) -> InlineKeyboardMarkup:
    rows = []
    total_pages = max(1, math.ceil(len(accounts) / page_size))
    current_page = min(max(1, page), total_pages)
    start = (current_page - 1) * page_size
    end = start + page_size
    page_accounts = accounts[start:end]
    rows.extend(
        [
            [
                InlineKeyboardButton(text="🔄 Авто-релогин", callback_data="acc:relogin"),
                InlineKeyboardButton(text="🌐 Контроль DC", callback_data="acc:dc"),
            ],
            [InlineKeyboardButton(text="🔄 Обновить список", callback_data=f"acc:session_manager:refresh:{current_page}")],
        ]
    )

    for item in page_accounts:
        state_icon = item.get("account_state_icon", "⚪")
        health_icon = item.get("health_icon", "⚪")
        pool_icon = "🟢" if item.get("in_pool", True) else "⛔"
        text = f"{item['index']}. {item['username']} {state_icon}{health_icon}{pool_icon}"
        rows.append([InlineKeyboardButton(text=text, callback_data=f"acc:view:{item['session']}:{current_page}")])

    if total_pages > 1:
        nav_row: list[InlineKeyboardButton] = []
        if current_page > 1:
            nav_row.append(
                InlineKeyboardButton(text="⬅️", callback_data=f"acc:session_manager:page:{current_page - 1}")
            )
        nav_row.append(
            InlineKeyboardButton(text=f"{current_page}/{total_pages}", callback_data=f"acc:session_manager:page:{current_page}")
        )
        if current_page < total_pages:
            nav_row.append(
                InlineKeyboardButton(text="➡️", callback_data=f"acc:session_manager:page:{current_page + 1}")
            )
        rows.append(nav_row)

    if accounts:
        rows.append([InlineKeyboardButton(text="🗑 Удалить доступные", callback_data=f"acc:bulk_delete:{current_page}")])

    rows.append([InlineKeyboardButton(text="⬅️ К аккаунтам", callback_data="menu:accounts")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def session_export_scope_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="1️⃣ Один", callback_data="acc:export_scope:single"),
                InlineKeyboardButton(text="🔢 Несколько", callback_data="acc:export_scope:multi"),
            ],
            [InlineKeyboardButton(text="🌍 Все", callback_data="acc:export_scope:all")],
            [InlineKeyboardButton(text="⬅️ К аккаунтам", callback_data="menu:accounts")],
        ]
    )


def session_export_format_keyboard(scope: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="📄 Обычные .session", callback_data=f"acc:export_format:{scope}:plain"),
            ],
            [
                InlineKeyboardButton(text="🗜 ZIP архив", callback_data=f"acc:export_format:{scope}:zip"),
            ],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="acc:export_menu")],
        ]
    )


def healthcheck_keyboard(
    *,
    accounts: list[dict],
    auto_remove_enabled: bool,
    notifications_enabled: bool,
    can_manage_owner_settings: bool,
    page: int,
    page_size: int,
) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    total_pages = max(1, math.ceil(len(accounts) / page_size))
    current_page = min(max(1, page), total_pages)
    start = (current_page - 1) * page_size
    end = start + page_size
    page_accounts = accounts[start:end]

    for offset in range(0, len(page_accounts), 2):
        row_items = page_accounts[offset:offset + 2]
        row: list[InlineKeyboardButton] = []
        for item in row_items:
            status = item.get("health_status")
            icon = "🟢" if status == "alive" else "🟡" if status == "limited" else "🔴" if status == "banned" else "⚪"
            username = str(item.get("username") or item.get("session") or "-")
            row.append(InlineKeyboardButton(text=f"{icon} {username}", callback_data=f"acc:view:{item['session']}:{current_page}"))
        rows.append(row)

    if total_pages > 1:
        nav_row: list[InlineKeyboardButton] = []
        if current_page > 1:
            nav_row.append(InlineKeyboardButton(text="⬅️", callback_data=f"acc:health:page:{current_page - 1}"))
        nav_row.append(InlineKeyboardButton(text=f"{current_page}/{total_pages}", callback_data=f"acc:health:page:{current_page}"))
        if current_page < total_pages:
            nav_row.append(InlineKeyboardButton(text="➡️", callback_data=f"acc:health:page:{current_page + 1}"))
        rows.append(nav_row)

    action_row = []
    if can_manage_owner_settings:
        action_row.append(InlineKeyboardButton(text="⚙️ Настройки", callback_data=f"acc:health:settings:{current_page}"))
    action_row.append(InlineKeyboardButton(text="🔄 Проверить", callback_data=f"acc:health:refresh:{current_page}"))
    rows.append(action_row)
    rows.append([InlineKeyboardButton(text="⬅️ К аккаунтам", callback_data="menu:accounts")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def activation_policies_keyboard(
    *,
    usage_policy_url: str,
    privacy_policy_url: str,
    terms_of_service_url: str,
) -> InlineKeyboardMarkup | None:
    usage = usage_policy_url.strip()
    privacy = privacy_policy_url.strip()
    terms = terms_of_service_url.strip()
    if not usage or not privacy or not terms:
        return None
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="Политика использования сервиса", url=usage),
                InlineKeyboardButton(text="Политика конфиденциальности", url=privacy),
            ],
            [
                InlineKeyboardButton(text="Пользовательское соглашение", url=terms),
            ],
        ]
    )


def health_settings_keyboard(*, page: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🛡 Переключить авто-вывод", callback_data=f"acc:health:toggle_auto_remove:{page}")],
            [InlineKeyboardButton(text="🔔 Переключить уведомления", callback_data=f"acc:health:toggle_notifications:{page}")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data=f"acc:health:page:{page}")],
        ]
    )


def account_add_mode_keyboard(action: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="⚡ Добавить сразу", callback_data=f"acc:{action}:mode:active")],
            [InlineKeyboardButton(text="⬅️ К аккаунтам", callback_data="menu:accounts")],
        ]
    )


def profile_keyboard(*, show_accounts: bool) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    if show_accounts:
        rows.append(
            [
                InlineKeyboardButton(text="👥 Аккаунты", callback_data="menu:accounts"),
            InlineKeyboardButton(text="🗂️ Задачи", callback_data="menu:tasks"),
            ]
        )
    else:
        rows.append([InlineKeyboardButton(text="🗂️ Задачи", callback_data="menu:tasks")])
    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="menu:home")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def dc_control_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🔄 Обновить DC", callback_data="acc:dc:refresh")],
            [InlineKeyboardButton(text="⬅️ К списку сессий", callback_data="acc:session_manager")],
        ]
    )


def help_back_keyboard(back_callback: str = "menu:home") -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="⬅️ Назад", callback_data=back_callback)],
        ]
    )


def task_created_keyboard(task_id: int, back_callback: str = "menu:tasks") -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="⏸ Пауза", callback_data=f"task:pause:{task_id}"),
                InlineKeyboardButton(text="🛑 Отменить", callback_data=f"task:cancel:{task_id}"),
            ],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data=back_callback)],
        ]
    )


def account_actions_keyboard(session: str, *, in_pool: bool, account_state: str, page: int) -> InlineKeyboardMarkup:
    pool_text = "⛔ Вывести из пула" if in_pool else "♻️ Вернуть в пул"
    rows = [
        [
            InlineKeyboardButton(text="✏️ Редактировать", callback_data=f"acc:edit:{session}:{page}"),
            InlineKeyboardButton(text="📤 Экспорт", callback_data=f"acc:export:{session}"),
        ],
        [InlineKeyboardButton(text="📖 Прочитать все", callback_data=f"acc:read_all:{session}:{page}")],
    ]

    rows.append([InlineKeyboardButton(text=pool_text, callback_data=f"acc:pool:{session}:{page}")])
    rows.append([InlineKeyboardButton(text="🗑 Удалить", callback_data=f"acc:delete:{session}")])
    rows.append([InlineKeyboardButton(text="⬅️ К списку сессий", callback_data=f"acc:session_manager:page:{page}")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def bulk_account_delete_keyboard(*, page: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🗑 Да, удалить всё доступное", callback_data=f"acc:bulk_delete:confirm:{page}")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data=f"acc:session_manager:page:{page}")],
        ]
    )


def account_edit_keyboard(session: str, page: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Изменить имя", callback_data=f"acc:edit_first_name:{session}")],
            [InlineKeyboardButton(text="Изменить фамилию", callback_data=f"acc:edit_last_name:{session}")],
            [InlineKeyboardButton(text="Изменить описание", callback_data=f"acc:edit_bio:{session}")],
            [InlineKeyboardButton(text="Изменить юзернейм", callback_data=f"acc:edit_username:{session}")],
            [InlineKeyboardButton(text="Изменить аватарку", callback_data=f"acc:edit_avatar:{session}")],
            [InlineKeyboardButton(text="Изменить дату рождения", callback_data=f"acc:edit_birthday:{session}")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data=f"acc:view:{session}:{page}")],
        ]
    )


def task_manager_keyboard(tasks: list[dict], *, has_finished: bool, back_callback: str = "menu:home") -> InlineKeyboardMarkup:
    rows = []

    for item in tasks:
        kind = item.get("kind", "task")
        rows.append([InlineKeyboardButton(text=f"#{item['id']} {kind}", callback_data=f"task:{item['id']}")])

    if has_finished:
        rows.append([InlineKeyboardButton(text="🧹 Очистить завершённые", callback_data="task:clear_finished")])
    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data=back_callback)])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def task_actions_keyboard(task_id: int, status: str, *, back_callback: str = "menu:tasks") -> InlineKeyboardMarkup:
    rows = []
    if status in {"queued", "running"}:
        rows.append(
            [
                InlineKeyboardButton(text="⏸ Пауза", callback_data=f"task_pause:{task_id}"),
                InlineKeyboardButton(text="🛑 Стоп", callback_data=f"task_stop:{task_id}"),
            ]
        )
    elif status == "paused":
        rows.append(
            [
                InlineKeyboardButton(text="▶️ Пуск", callback_data=f"task_restart:{task_id}"),
                InlineKeyboardButton(text="🛑 Стоп", callback_data=f"task_stop:{task_id}"),
            ]
        )
    else:
        rows.append([InlineKeyboardButton(text="🔄 Перезапуск", callback_data=f"task_restart:{task_id}")])

    if status not in {"paused", "completed", "failed", "canceled"}:
        rows.append([InlineKeyboardButton(text="🔄 Перезапуск", callback_data=f"task_restart:{task_id}")])
    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data=back_callback)])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def admin_confirm_stop_all_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="✅ Остановить", callback_data="confirm_stop_all")],
            [InlineKeyboardButton(text="⬅️ Отмена", callback_data="back")],
        ]
    )
