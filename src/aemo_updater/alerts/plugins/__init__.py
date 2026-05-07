"""Alert plugins. Each plugin inspects data and emits Alerts.

A plugin is any object with a `name: str` attribute and an
`evaluate(ctx: AlertContext) -> list[Alert]` method. See
docs/alerts_plugin_architecture.md for the contract.
"""
