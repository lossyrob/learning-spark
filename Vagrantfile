Vagrant.configure("2") do |config|
  config.vm.box = "ubuntu/trusty64"
  config.vm.box_url = 
    "http://cloud-images.ubuntu.com/vagrant/trusty/current/trusty-server-cloudimg-amd64-vagrant-disk1.box"

  config.vm.define "default" do |devmachine|
    devmachine.vm.network :private_network, ip: "192.168.88.88"
    devmachine.vm.network "forwarded_port", guest: 4040, host: 4040

    devmachine.vm.provider :virtualbox do |v|
      v.name = "learning-spark"
      v.memory = 2048
      v.cpus = 8
    end
  end
end
