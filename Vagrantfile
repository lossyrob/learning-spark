Vagrant.configure("2") do |config|
  config.vm.box = "ubuntu/trusty64"
  config.vm.box_url = "http://cloud-images.ubuntu.com/vagrant/trusty/current/trusty-server-cloudimg-amd64-vagrant-disk1.box"

  config.vm.provider :virtualbox do |v|
    v.name = "coding-with-spark"
    v.memory = 2048
    v.cpus = 8
  end

  # config.vm.provision :ansible do |ansible|
  #   ansible.playbook = "deploy/dev-machine.yml"
  #   ansible.inventory_path = "deploy/hosts"
  #   ansible.limit = "192.168.88.88"
  # end
end
