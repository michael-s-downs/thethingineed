data "azurerm_private_dns_zone" "dns_zone" {
  name                = var.private_dns_zones[var.resource]
  resource_group_name = var.rg_dns
}

data "azurerm_virtual_network" "vnet" {
  name                = var.vnet
  resource_group_name = var.rg_dns
}

data "azurerm_subnet" "subnet" {
  name                 = var.subnet
  resource_group_name  = var.rg_dns
  virtual_network_name = data.azurerm_virtual_network.vnet.name
}
