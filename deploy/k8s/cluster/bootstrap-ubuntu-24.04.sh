#!/usr/bin/env bash
set -euo pipefail

# Run inside a dedicated Ubuntu 24.04 VM as a sudo-capable user.
sudo swapoff -a
sudo sed -i '/\sswap\s/s/^/#/' /etc/fstab
printf 'overlay\nbr_netfilter\n' | sudo tee /etc/modules-load.d/airtraffic-k8s.conf >/dev/null
sudo modprobe overlay
sudo modprobe br_netfilter
printf 'net.bridge.bridge-nf-call-iptables = 1\nnet.ipv4.ip_forward = 1\n' |
  sudo tee /etc/sysctl.d/99-airtraffic-k8s.conf >/dev/null
sudo sysctl --system >/dev/null

sudo apt-get update
sudo DEBIAN_FRONTEND=noninteractive apt-get install -y ca-certificates curl gnupg containerd
sudo mkdir -p /etc/containerd
containerd config default | sudo tee /etc/containerd/config.toml >/dev/null
sudo sed -i 's/SystemdCgroup = false/SystemdCgroup = true/' /etc/containerd/config.toml
grep -q 'SystemdCgroup = true' /etc/containerd/config.toml
sudo systemctl enable --now containerd
sudo systemctl restart containerd

sudo install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://pkgs.k8s.io/core:/stable:/v1.37/deb/Release.key |
  sudo gpg --dearmor --yes -o /etc/apt/keyrings/kubernetes-apt-keyring.gpg
printf 'deb [signed-by=/etc/apt/keyrings/kubernetes-apt-keyring.gpg] https://pkgs.k8s.io/core:/stable:/v1.37/deb/ /\n' |
  sudo tee /etc/apt/sources.list.d/kubernetes.list >/dev/null
sudo apt-get update
version=$(apt-cache madison kubeadm | awk '$3 ~ /^1\.37\.2-/ {v=$3} END {print v}')
if [[ -z "$version" ]]; then
  echo 'Pinned Kubernetes package version 1.37.2 is unavailable' >&2
  exit 1
fi
sudo DEBIAN_FRONTEND=noninteractive apt-get install -y \
  "kubelet=$version" "kubeadm=$version" "kubectl=$version"
sudo apt-mark hold kubelet kubeadm kubectl
sudo systemctl enable kubelet
printf '%s\n' "$version" | sudo tee /etc/airtraffic-kubernetes-package-version >/dev/null
kubeadm version -o short
containerd --version
