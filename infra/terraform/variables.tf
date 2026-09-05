variable "project_name" {
  description = "Short name used as a prefix for all resource names"
  type        = string
  default     = "commissions"
}

variable "environment" {
  description = "Deployment environment: dev, staging, or prod"
  type        = string
  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "environment must be one of: dev, staging, prod."
  }
}

variable "location" {
  description = "Azure region"
  type        = string
  default     = "eastus2"
}

variable "storage_replication_type" {
  description = "ADLS Gen2 replication (LRS is cheapest — fine for a portfolio project)"
  type        = string
  default     = "LRS"
}

variable "databricks_sku" {
  description = "premium is required for Unity Catalog / credential passthrough; standard is cheaper for a quick local demo"
  type        = string
  default     = "premium"
}

variable "tags" {
  description = "Common resource tags"
  type        = map(string)
  default = {
    project = "insurance-agent-commissions"
    owner   = "data-engineering"
  }
}
