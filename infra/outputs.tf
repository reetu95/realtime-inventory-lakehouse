output "resource_group" {
  value = azurerm_resource_group.rg.name
}

output "adls_account" {
  value = azurerm_storage_account.adls.name
}

output "key_vault_uri" {
  value = azurerm_key_vault.kv.vault_uri
}
