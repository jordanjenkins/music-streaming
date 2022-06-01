#!/bin/bash

# Run using following command
# bash vm_setup.sh

echo "Downloading anaconda"
wget https://repo.anaconda.com/archive/Anaconda3-2022.05-Linux-x86_64.sh

echo "Running anaconda script"
bash Anaconda3-2022.05-Linux-x86_64.sh -b -p ~/anaconda

echo "Removing anaconda script"
rm Anaconda3-2022.05-Linux-x86_64.sh

# activate conda
eval "$($HOME/anaconda/bin/conda shell.bash hook)"

echo "Running conda init"
conda init

echo "Running conda update"
conda update -y conda

echo "Installed conda version:"
conda --version

echo "Running sudo apt-get update"
sudo apt-get update

echo "Installing Docker"
sudo apt-get -y install docker.io

echo "Docker without sudo setup"
sudo group add docker
sudo gpasswd -a $USER docker
sudo service docker restart

echo "Installing docker-compose"
cd
mkdir -p bin
cd bin
wget https://github.com/docker/compose/releases/download/v2.6.0/docker-compose-linux-x86_64 -O docker-compose
sudo chmod +x docker-compose

echo "Setup .bashrc"
echo '' >> ~/.bashrc
echo 'export PATH=${HOME}/bin:${PATH}' >> ~/.bashrc
eval "$(cat ~/.bashrc | tail -n +10)" # A hack because source .bashrc doesn't work inside the script

echo "docker-compose version:"
docker-compose --version

mkdir -p ~/.google/credentials