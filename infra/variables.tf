variable "project" {
  default = "rtinv"
}

variable "location" {
  default = "eastus2"
}

variable "kafka_bootstrap" {
  type      = string
  sensitive = true
}

variable "kafka_api_key" {
  type      = string
  sensitive = true
}

variable "kafka_api_secret" {
  type      = string
  sensitive = true
}
