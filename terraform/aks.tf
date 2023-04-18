data "azurerm_resource_group" "nwm" {
  name = "tom-noaanwm-test"
}


resource "azurerm_virtual_network" "nwm" {
  name                = "tom-noaanwm-test-network"
  location            = data.azurerm_resource_group.nwm.location
  resource_group_name = data.azurerm_resource_group.nwm.name
  address_space       = ["10.0.0.0/8"]
  tags                = {}
}

resource "azurerm_subnet" "node_subnet" {
  name                 = "tom-noaanwm-test-node-subnet"
  virtual_network_name = azurerm_virtual_network.nwm.name
  resource_group_name  = data.azurerm_resource_group.nwm.name
  address_prefixes     = ["10.1.0.0/16"]
}

resource "azurerm_network_security_group" "nwm" {
  name                = "tom-noaanwm-test-security-group"
  location            = data.azurerm_resource_group.nwm.location
  resource_group_name = data.azurerm_resource_group.nwm.name

  security_rule {
    name                       = "hub-rule"
    priority                   = 100
    direction                  = "Inbound"
    access                     = "Allow"
    protocol                   = "Tcp"
    source_port_range          = "*"
    destination_port_ranges    = ["80", "443", "8786", "8787"]
    source_address_prefix      = "*"
    destination_address_prefix = "*"
  }
}

resource "azurerm_subnet_network_security_group_association" "nwm" {
  subnet_id                 = azurerm_subnet.node_subnet.id
  network_security_group_id = azurerm_network_security_group.nwm.id
}

resource "azurerm_kubernetes_cluster" "nwm" {
  name                = "aks-noaanwm-test"
  location            = data.azurerm_resource_group.nwm.location
  resource_group_name = data.azurerm_resource_group.nwm.name
  dns_prefix          = "noaanwm-test"

  azure_active_directory_role_based_access_control {
    managed            = true
    azure_rbac_enabled = true
  }

  # Core node-pool
  default_node_pool {
    name           = "core"
    vm_size        = "Standard_D2_v2"
    node_count     = 1
    vnet_subnet_id = azurerm_subnet.node_subnet.id
    node_labels = {
      "hub.jupyter.org/node-purpose" = "core"
    }
  }

  auto_scaler_profile {
    empty_bulk_delete_max       = "50"
    scale_down_unready          = "2m"
    scale_down_unneeded         = "2m"
    scale_down_delay_after_add  = "5m"
    skip_nodes_with_system_pods = false # ensures system pods don't keep GPU nodes alive
  }
  identity {
    type = "SystemAssigned"
  }


  lifecycle {
    prevent_destroy = true
  }

}


resource "azurerm_kubernetes_cluster_node_pool" "worker_pool" {
  name                  = "cpuworker"
  kubernetes_cluster_id = azurerm_kubernetes_cluster.nwm.id
  vm_size               = "Standard_E8_v3"
  enable_auto_scaling   = true
  priority              = "Spot"
  eviction_policy       = "Delete"
  spot_max_price        = -1
  vnet_subnet_id        = azurerm_subnet.node_subnet.id

  node_labels = {
    "k8s.dask.org/dedicated"                = "worker",
    "kubernetes.azure.com/scalesetpriority" = "spot"
  }

  node_taints = [
    "kubernetes.azure.com/scalesetpriority=spot:NoSchedule",
  ]

  min_count = 0
  max_count = 100

  lifecycle {
    ignore_changes = [
      node_count,
    ]
  }

}
