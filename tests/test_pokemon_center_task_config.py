import importlib.util
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _load_app_module(tmp_path, monkeypatch):
    db_path = tmp_path / 'pc-task-config.db'
    monkeypatch.setenv('DB_PATH', str(db_path))
    module_name = 'app_pc_task_config_test'
    if module_name in sys.modules:
        del sys.modules[module_name]
    spec = importlib.util.spec_from_file_location(module_name, ROOT / 'app.py')
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    module.init_db()
    return module


def test_pokemon_center_defaults_are_applied_for_pokemon_center_only(tmp_path, monkeypatch):
    app_module = _load_app_module(tmp_path, monkeypatch)

    pokemon_monitor = {'retailer': 'pokemoncenter', 'product_url': 'https://www.pokemoncenter.com/product/123'}
    normalized = app_module.normalize_task_config_for_monitor(
        {'products': [{'sku': 'abc'}]},
        monitor_row=pokemon_monitor,
    )

    assert normalized['site'] == 'us'
    assert normalized['mode'] == 'default'
    assert normalized['monitor_delay_ms'] == 3500
    assert normalized['product_quantity'] == 1
    assert normalized['wait_for_queue'] is False
    assert normalized['loop_checkout'] is False
    assert normalized['group_limits']['antibot_event_threshold'] == 3
    assert normalized['group_limits']['antibot_cooldown_seconds'] == 60
    assert normalized['products'][0]['skip_if_oos'] is False

    walmart_monitor = {'retailer': 'walmart', 'product_url': 'https://www.walmart.com/ip/1'}
    generic = app_module.normalize_task_config_for_monitor({}, monitor_row=walmart_monitor)
    assert 'mode' not in generic
    assert 'monitor_delay_ms' not in generic
    assert 'task_group_version' not in generic


def test_pokemon_center_legacy_task_config_gets_fallback_values_on_read(tmp_path, monkeypatch):
    app_module = _load_app_module(tmp_path, monkeypatch)

    conn = app_module.db()
    now = app_module.utc_now()
    monitor_id = conn.execute(
        """
        insert into monitors(workspace_id, retailer, category, product_url, poll_interval_seconds, created_at)
        values (1, 'pokemoncenter', 'pokemon', 'https://www.pokemoncenter.com/product/123', 20, ?)
        """,
        (now,),
    ).lastrowid
    conn.execute(
        """
        insert into checkout_tasks(
            workspace_id, monitor_id, task_name, task_config, current_state, enabled, is_paused, created_at, updated_at, last_transition_at
        ) values (?, ?, 'legacy', ?, 'queued', 0, 0, ?, ?, ?)
        """,
        (1, monitor_id, json.dumps({'retailer': 'pokemoncenter'}), now, now, now),
    )
    conn.commit()

    row = conn.execute(
        "select * from checkout_tasks where monitor_id = ?",
        (monitor_id,),
    ).fetchone()
    payload = app_module.serialize_checkout_task(row)['task_config']
    assert payload['mode'] == 'default'
    assert payload['monitor_delay_ms'] == 3500
    assert payload['task_group_version'] == app_module.POKEMON_CENTER_TASK_GROUP_SCHEMA_VERSION
    conn.close()


def test_mode_specific_requirements_and_site_support(tmp_path, monkeypatch):
    app_module = _load_app_module(tmp_path, monkeypatch)

    create_account_config = app_module.normalize_task_config_for_monitor(
        {
            'mode': 'create_account',
            'site': 'us',
            'profile_email': 'ash@example.com',
            'profile_first_name': 'Ash',
            'profile_last_name': 'Ketchum',
            'account_output_target': 'accounts/us.csv',
            'profile': 'should-be-ignored',
            'payment': 'should-be-ignored',
        },
        monitor_row={'retailer': 'pokemoncenter', 'product_url': 'https://www.pokemoncenter.com/product/123'},
    )
    assert app_module.validate_pokemon_center_mode_requirements(create_account_config) is None
    assert create_account_config['profile'] is None
    assert create_account_config['payment'] is None
    assert app_module.validate_pokemon_center_mode_site('create_account', 'ca') == "Unsupported site 'ca' for mode 'create_account'"


def test_create_account_mode_executes_without_checkout_binding(tmp_path, monkeypatch):
    app_module = _load_app_module(tmp_path, monkeypatch)
    conn = app_module.db()
    now = app_module.utc_now()
    monitor_id = conn.execute(
        """
        insert into monitors(workspace_id, retailer, category, product_url, poll_interval_seconds, created_at)
        values (1, 'pokemoncenter', 'pokemon', 'https://www.pokemoncenter.com/product/123', 20, ?)
        """,
        (now,),
    ).lastrowid
    task = app_module.create_checkout_task(
        conn,
        workspace_id=1,
        monitor_id=monitor_id,
        task_config={
            'mode': 'create_account',
            'site': 'us',
            'profile_email': 'ash@example.com',
            'profile_first_name': 'Ash',
            'profile_last_name': 'Ketchum',
            'account_output_target': 'accounts/us.csv',
        },
    )
    conn.commit()
    conn.close()

    updated = app_module.execute_checkout_task_state_machine(task['id'], 1)
    assert updated is not None
    assert updated['current_state'] == 'success'
