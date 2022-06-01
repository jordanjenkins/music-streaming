
variable "project" {
  description = "music-streaming-jenkins"
  default     = "music-streaming-jenkins"
  type        = string
}

variable "region" {
  description = "us-central1"
  default     = "us-central1"
  type        = string
}

variable "zone" {
  description = "us-central-1a"
  default     = "us-central1-a"
  type        = string
}

variable "network" {
  description = "Network for your instance/cluster"
  default     = "default"
  type        = string
}

variable "vm_image" {
  description = "Image for your VM"
  default     = "ubuntu-os-cloud/ubuntu-2004-lts"
  type        = string
}

variable "bucket" {
  description = "music-streaming-jenkins"
  type        = string
}

variable "stg_bq_dataset" {
  description = "Storage class type for your bucket. Check official docs for more info."
  default     = "streams_stg"
  type        = string
}

variable "prod_bq_dataset" {
  description = "Storage class type for your bucket. Check official docs for more info."
  default     = "streams_prod"
  type        = string
}