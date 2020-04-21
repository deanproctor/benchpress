sudo yum -y groupinstall development
sudo yum -y install zlib-devel openssl-devel docker jq

systemctl start docker

wget https://www.openssl.org/source/openssl-1.1.1f.tar.gz
tar xzvf openssl-1.1.1f.tar.gz
cd openssl-1.1.1f/

./config shared
make
sudo make install
export LD_LIBRARY_PATH=/usr/local/ssl/lib/

cd ..
rm openssl-1.1.1f.tar.gz
rm -rf openssl-1.1.1f

# Install Python 3.6
wget https://www.python.org/ftp/python/3.6.10/Python-3.6.10.tar.xz
tar xJf Python-3.6.10.tar.xz
cd Python-3.6.10

./configure
make
sudo make install

cd ..
rm Python-3.6.10.tar.xz
sudo rm -rf Python-3.6.10

# Create virtualenv running Python 3.6
sudo /usr/local/bin/pip3 install virtualenv
virtualenv -p python3 MYVENV
source MYVENV/bin/activate

python --version
# Python 3.6.0

pip install streamsets-testframework
pip install streamsets-testenvironments
pip install psutil

sudo ln -sf /usr/share/zoneinfo/UTC /etc/localtime

mkdir -p ~/.streamsets/activation

echo "Install activation key to ~/.streamsets/activation/rsa-signed-activation-info.properties"
