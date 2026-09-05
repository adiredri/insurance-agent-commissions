output "resource_group_name" {
  value = azurerm_resource_group.this.name
}

output "storage_account_name" {
  value = azurerm_storage_account.lakehouse.name
}

output "lakehouse_abfss_url" {
  description = "Pass this to LAKEHOUSE_ROOT so pipeline code writes straight to ADLS"
  value       = "abfss://lakehouse@${azurerm_storage_account.lakehouse.name}.dfs.core.windows.net"
}

output "raw_landing_abfss_url" {
  value = "abfss://raw@${azurerm_storage_account.lakehouse.name}.dfs.core.windows.net"
}

output "databricks_workspace_url" {
  value = azurerm_databricks_workspace.this.workspace_url
}

output "key_vault_uri" {
  value = azurerm_key_vault.this.vault_uri
}
