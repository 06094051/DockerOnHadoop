# -*- coding:utf-8 -*-
import argparse
import sys
from docker_container_executor_impl import *

INITIALIZE_CONTAINER = 0
LAUNCH_CONTAINER = 1
SIGNAL_CONTAINER = 2
DELETE_AS_USER = 3
RENAME_LOCAL_FOR_CLEANUP = 4
WAIT_CONTAINER = 5
CLEAN_IMAGE_AND_TMP = 6

parser = argparse.ArgumentParser()
parser.add_argument("-t", '--type', default=INITIALIZE_CONTAINER)
parser.add_argument("-u", "--user", default="sankuai", help="userfor execute the program.")

parser.add_argument("-app_id", "--app-id", help="app id.")
parser.add_argument("-container-id", "--container-id", help="container id.")
parser.add_argument("-work-dir", "--work-dir", help="app or container working dir.")
parser.add_argument("-local-dirs", "--local-dirs", help="local dirs for application.")
parser.add_argument("-log-dirs", "--log-dirs", help="log dirs for app.")
parser.add_argument("-token-path", "--token-path", help="token path.")

parser.add_argument("-v", "--volumes", help="docker need volumes path.")

# init only
parser.add_argument("-loc-id", "--loc-id", help="loc id")
parser.add_argument("-cmd", "--cmd", help="use to localizer.")
# lanuch only
parser.add_argument("-script-path", "--script-path", help="script path.")
parser.add_argument("-pid-path", "--pid-path", help="container pid path.")
parser.add_argument("-docker-exec", "--docker-exec", help="docker exec path.")
parser.add_argument("-image-name", "--image-name", help="docker image name.")
parser.add_argument("-gpus", "--gpus", help="gpus for container.")
# del only
parser.add_argument("-del-dirs", "--del-dirs", help="del dirs path for del.")
parser.add_argument("-conf", "--conf", help="config file.", default='../etc/hadoop/docker_container_executor.yml')

parser.add_argument("-signal", "--signal", help="signal no.", default=None)
parser.add_argument("-pid", "--pid", help="pid.")

# RENAME_LOCAL_FOR_CLEANUP Only
parser.add_argument("-s", "--source-path", help="source path.", default=None)
parser.add_argument("-d", "--dest-path", help="dest path.")


parser.add_argument("-cpu", "--cpu", help="cpu cores.", default=8)
parser.add_argument("-mem", "--memory", help="memory", default=32768)
args = parser.parse_args()

print ("exec {0}".format(sys.argv))

_type = int(args.type)
conf = load_config(args.conf)
if _type == INITIALIZE_CONTAINER:
  if not conf:
    print("can not get config.")
    sys.exit(1)
  user_name = args.user
  app_id = args.app_id

  if not app_id:
    print ("--app_id is None.")
    sys.exit(1)

  work_dir = args.work_dir
  if not work_dir:
    print ("--work_dir is None.")
    sys.exit(1)

  loc_id = args.loc_id
  if not loc_id:
    print ("--loc_id is None.")
    sys.exit(1)
  token = args.token_path
  if not token:
    print ("--token_path is None")
    sys.exit(1)

  log = args.log_dirs
  if log:
    log_dirs = log.split(",")
    if "" in log_dirs:
      log_dirs.remove("")
    if log_dirs:
      conf["yarn.nodemanager.log-dirs"] = log_dirs
  local = args.local_dirs
  if local:
    local_dirs = local.split(",")
    if "" in local_dirs:
      local_dirs.remove("")
    if local_dirs:
      conf["yarn.nodemanager.local-dirs"] = local_dirs
  volumes_str = args.volumes
  volumes = []
  if volumes_str:
    volumes = volumes_str.split(",")
  cmd = args.cmd
  if not cmd:
    print ("--token_path is None, start_localizer need --cmd to localizer.")
    sys.exit(1)
  cmd_shell = cmd.replace(",", "  ")
  image_name = args.image_name
  print("start localizer use image {0}".format(image_name))
  status = start_localizer(nm_private_container_tokens_path=token, working_dir=work_dir, app_id=app_id, loc_id=loc_id,
                           cmd_shell=cmd_shell, user_name=user_name, image_name=image_name, volumes=volumes, conf=conf)
  if not status:
    print ("start_localizer fail.")
    sys.exit(1)
  sys.exit(0)
elif _type == LAUNCH_CONTAINER:
  if not conf:
    print("can not get config.")
    sys.exit(1)
  user_name = args.user

  app_id = args.app_id
  if not app_id:
    print ("--app_id is None.")
    sys.exit(1)

  container_id = args.container_id
  if not container_id:
    print ("--container_id is None.")
    sys.exit(1)

  work_dir = args.work_dir
  if not work_dir:
    print ("--work_dir is None.")
    sys.exit(1)

  token = args.token_path
  if not token:
    print ("--token_path is None.")
    sys.exit(1)

  script_path = args.script_path
  if not script_path:
    print ("--script_path is None.")
    sys.exit(1)

  image_name = args.image_name
  if not image_name:
    print ("--image_name is None.")
    sys.exit(1)

  pid_path = args.pid_path
  if not pid_path:
    print ("--pid_path is None.")
    sys.exit(1)

  gpu = args.gpus
  gpus = []
  if gpu:
    gpus = gpu.split(",")
    print ("use gpus:{0}".format(gpus))
  
  cpu = args.cpu
  memory = args.memory
  
  log = args.log_dirs
  if log:
    log_dirs = log.split(",")
    if "" in log_dirs:
      log_dirs.remove("")
    if log_dirs:
      conf["yarn.nodemanager.log-dirs"] = log_dirs
  local = args.local_dirs
  if local:
    local_dirs = local.split(",")
    if "" in local_dirs:
      local_dirs.remove("")
    if local_dirs:
      conf["yarn.nodemanager.local-dirs"] = local_dirs
  volumes_str = args.volumes
  volumes = []
  if volumes_str:
    volumes = volumes_str.split(",")
  status = launch_container(nm_private_container_script_path=script_path, nm_private_tokens_path=token,
                            container_working_dir=work_dir, app_id=app_id, container_id=container_id,
                            image_name=image_name, pid_path=pid_path, user_name=user_name, cpu=cpu, memory=memory,
                            gpus=gpus, volumes=volumes, conf=conf)
  if not status:
    print ("launch_container fail.")
    sys.exit(1)
  sys.exit(0)
elif _type == DELETE_AS_USER:
  if not conf:
    print("can not get config.")
    sys.exit(1)

  user_name = args.user
  del_str = args.del_dirs
  if not del_str:
    print ("--del_dirs is None.")
    sys.exit(1)
  del_paths = del_str.split(",")
  if "" in del_paths:
    del_paths.remove("")
  dels = []
  for _path in del_paths:
    if _path.startswith("file:"):
      dels.append(_path[5:])
    elif _path.startswith("/"):
      dels.append(_path)
    else:
      print ("{0} is not local file, cannot delete".format(_path))
  if not dels:
    sys.exit(0)
  del_as_user(user_name=user_name, del_paths=dels, conf=conf)
  sys.exit(0)
elif _type == SIGNAL_CONTAINER:
  if not conf:
    print("can not get config.")
    sys.exit(1)

  pid = args.pid
  if not pid:
    print("pid is None.")
    sys.exit(1)

  sig = args.signal
  if not sig:
    print("signal is None.")
    sys.exit(1)
  signal = int(sig)
  signal_container(pid=pid, signal=signal, conf=conf)
  sys.exit(0)
elif _type == RENAME_LOCAL_FOR_CLEANUP:
  if not conf:
    print("can not get config.")
    sys.exit(1)

  source_path = args.source_path
  if not source_path:
    print("--source_path is None")
    sys.exit(1)

  dest_path = args.dest_path
  if not dest_path:
    print("--dest_path is None")
    sys.exit(1)
  rename_local_file_for_cleanup(conf, source_path, dest_path)
  sys.exit(0)
elif _type == WAIT_CONTAINER:
  user_name = args.user
  container_id = args.container_id
  exit_code = wait_container(conf=conf, container_id=container_id, user_name=user_name)
  sys.exit(exit_code)
elif _type == CLEAN_IMAGE_AND_TMP:
  local = args.local_dirs
  if local:
    local_dirs = local.split(",")
    if "" in local_dirs:
      local_dirs.remove("")
    if local_dirs:
      conf["yarn.nodemanager.local-dirs"] = local_dirs
  clean_images_and_tmp(conf=conf)
else:
  print ("Invalid command %d not supported.{0}".format(sys.argv))
  sys.exit(1)
