locals {
  name_prefix = "${var.project_name}-${var.environment}"
  # storage account names must be globally unique, lowercase, no hyphens, <= 24 chars
  storage_account_name = substr(replace("st${var.project_name}${var.environment}", "-", ""), 0, 24)
  tags                 = merge(var.tags, { environment = var.environment })
}

resource "azurerm_resource_group" "this" {
  name     = "rg-${local.name_prefix}"
  location = var.location
  tags     = local.tags
}

# ---------------------------------------------------------------------------
# ADLS Gen2 — the lakehouse storage backing the medallion (raw/bronze/silver/gold)
# ---------------------------------------------------------------------------
resource "azurerm_storage_account" "lakehouse" {
  name                     = local.storage_account_name
  resource_group_name      = azurerm_resource_group.this.name
  location                 = azurerm_resource_group.this.location
  account_tier             = "Standard"
  account_replication_type = var.storage_replication_type
  account_kind             = "StorageV2"
  is_hns_enabled           = true # hierarchical namespace = ADLS Gen2, not plain blob storage
  min_tls_version          = "TLS1_2"
  tags                     = local.tags
}

resource "azurerm_storage_data_lake_gen2_filesystem" "lakehouse" {
  name               = "lakehouse"
  storage_account_id = azurerm_storage_account.lakehouse.id
}

resource "azurerm_storage_data_lake_gen2_filesystem" "raw" {
  name               = "raw"
  storage_account_id = azurerm_storage_account.lakehouse.id
}

# ---------------------------------------------------------------------------
# Key Vault — secret backing for Databricks secret scopes (DB connection
# strings, service principal secrets, etc.) — never commit secrets to git.
# ---------------------------------------------------------------------------
resource "azurerm_key_vault" "this" {
  name                       = "kv-${substr(local.name_prefix, 0, 20)}"
  resource_group_name        = azurerm_resource_group.this.name
  location                   = azurerm_resource_group.this.location
  tenant_id                  = data.azurerm_client_config.current.tenant_id
  sku_name                   = "standard"
  purge_protection_enabled   = false # true in a real prod environment; false so `terraform destroy` cleans up fully
  soft_delete_retention_days = 7
  enable_rbac_authorization  = true
  tags                       = local.tags
}

data "azurerm_client_config" "current" {}

# ---------------------------------------------------------------------------
# Azure Databricks workspace
# ---------------------------------------------------------------------------
resource "azurerm_databricks_workspace" "this" {
  name                = "dbw-${local.name_prefix}"
  resource_group_name = azurerm_resource_group.this.name
  location            = azurerm_resource_group.this.location
  sku                 = var.databricks_sku
  tags                = local.tags
}

# Grant the Databricks workspace's managed identity access to the lakehouse storage.
resource "azurerm_role_assignment" "databricks_storage_contributor" {
  scope                = azurerm_storage_account.lakehouse.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_databricks_workspace.this.storage_account_identity[0].principal_id
}
