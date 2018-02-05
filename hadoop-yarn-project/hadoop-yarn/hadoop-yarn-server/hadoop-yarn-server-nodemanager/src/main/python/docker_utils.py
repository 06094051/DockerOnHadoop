import docker
import time
import os
import traceback

BUILD = 0
PULL = 1
DOCKERFILE = 2
IGNORE = 3


class Docker(object):
  def __init__(self, conf, user_name="test", uid=10000, group_name="mos", group_id=8888):
    self.conf = conf
    self.op_type = conf.get("docker.image.get.type", BUILD)
    self.base_url = conf.get("docker.opts.url", 'unix:///var/run/docker.sock')
    self.timeout = conf.get("docker.timeout", 20)
    self.client = docker.Client(base_url=self.base_url, timeout=self.timeout)
    self.user_name = user_name
    self.uid = uid
    self.group_name = group_name
    self.group_id = group_id
    self.docker_exec = self.conf.get("yarn-nodemanager.docker-executor.path", "/usr/bin/docker")
    self.default_localizer_image_name = self.conf.get("yarn-nodemanager.docker.localizer.image.name", "mos-localizer")
    self.docker_registry = self.conf.get("docker.registry", "")

  def list_images(self, image_name):
    if ":" not in image_name:
      image_name = "{0}:latest".format(image_name)
    images = self.client.images(image_name)
    if not len(images):
      return None
    return images[0]

  def pull_image(self, image_name):
    ret_time = 0
    while ret_time < 3:
      print ("pull {0}".format(image_name))
      # self.client.pull(image_name)
      if not os.system("{0} pull {1}".format(self.docker_exec, image_name)):
        break
      ret_time += 1
    image = self.list_images(image_name)
    if image:
      print ("pull {0} successfully.".format(image))
    else:
      print ("pull {0} failed.".format(image))
    return image


  def list_container_id_by_pid(self, pid):
    containers = self.client.containers()
    for container in containers:
      container_id = container.get('Id', None)
      if container_id:
        _id = os.popen('{0} inspect --format {1} {2}'.format(self.docker_exec, '{{.State.Pid}}', str(container_id)))
        if pid in _id.read():
          return str(container_id)
    return None

  def stop_container(self, container_id):
    if container_id:
      self.client.stop(container_id)
      # os.system("{0} rm --force {1}".format(self.docker_exec, container_id))

  def stop_container_by_pid(self, pid):
    try:
      container_id = self.list_container_id_by_pid(pid)
      self.stop_container(container_id)
    except:
      os.system("kill -9 {0}".format(pid))
      print("stop container failed, use kill -9 {0},.....{1}".format(pid, traceback.format_exc()))

  def wait_container(self, container_id):
    return self.client.wait(container=container_id)

  def list_running_user(self):
    containers = self.client.containers()
    users = set()
    for container in containers:
      try:
        inspect_container = self.client.inspect_container(container["Id"])
        users.add(inspect_container["Config"]["User"])
      except:
        print("inspect container is failed.".format(container))
    return users

  def clean_images(self):
    running_images = set()
    try:
      containers = self.client.containers()
      for container in containers:
        running_images.add(container["ImageID"])
    except:
      print('list running container failed.')
    del_images = set()
    images = self.client.images()
    for image in images:
      try:
        if not image["RepoTags"] or image["RepoTags"] == ["<none>:<none>"]:
          del_images.add(image["Id"])
          continue
        inspect = self.client.inspect_image(image["Id"])
        if inspect["Config"]["User"] and inspect["Config"]["User"] != "root":
          print(inspect, "add del images list.")
          del_images.add(image["Id"])
      except:
        print("remove {0} failed: {1}".format(image, traceback.format_exc()))
    print("del image size:".format(len(del_images)), del_images)
    num_images = self.conf.get("nm.user.images.cache", 10)
    if len(del_images) > num_images:
      print("Images too much, clean user images")
      for image in del_images:
        if image in running_images:
          print("{0} is running, not remove".format(image))
          continue
        try:
          self.client.remove_image(image, force=True)
        except:
          print("remove image {0} failed:{1}".format(images, traceback.format_exc()))



