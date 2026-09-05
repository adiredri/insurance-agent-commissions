terraform {
  required_version = ">= 1.7.0"

  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.100"
    }
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.51"
    }
  }

  # Remote state — bootstrap this storage account/container once by hand (or via
  # infra/terraform/bootstrap/) before pointing Terraform at it; you can't create
  # your own state backend from within the state it will hold.
  # backend "azurerm" {
  #   resource_group_name  = "rg-tfstate"
  #   storage_account_name = "sttfstatecommissions"
  #   container_name       = "tfstate"
  #   key                  = "commissions-pipeline.tfstate"
  # }
}

provider "azurerm" {
  features {
    key_vault {
      purge_soft_delete_on_destroy    = true
      recover_soft_deleted_key_vaults = true
    }
  }

  # Auth via OIDC federated credentials from GitHub Actions — no long-lived secrets.
  # Set ARM_CLIENT_ID / ARM_TENANT_ID / ARM_SUBSCRIPTION_ID as env vars or GitHub secrets;
  # ARM_USE_OIDC=true and ARM_OIDC_TOKEN are supplied automatically by azure/login in CI.
  use_oidc = true
}

provider "databricks" {
  host = azurerm_databricks_workspace.this.workspace_url
}
