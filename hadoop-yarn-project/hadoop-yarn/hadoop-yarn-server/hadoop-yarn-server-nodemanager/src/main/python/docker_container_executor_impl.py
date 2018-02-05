import time
import docker_utils
from utils import *

SIGKILL = 9


def start_localizer(nm_private_container_tokens_path, working_dir, app_id, loc_id, cmd_shell, user_name,
                    image_name=None, volumes=[], conf={}):
  try:
    user_name, uid, group_name, group_id = get_user_info(user_name, conf)
    log_dirs = conf.get("yarn.nodemanager.log-dirs", [])
    if not log_dirs:
      print("yarn.nodemanager.log-dirs not set.")
      sys.exit(1)

    local_dirs = conf.get("yarn.nodemanager.local-dirs", [])
    if not local_dirs:
      print("yarn.nodemanager.local-dirs not set.")
      sys.exit(1)

    yarn_name = conf.get("yarn.nodemanager.docker-container-executor.group", "sankuai")
    yarn_info = pwd.getpwnam(yarn_name)
    yarn_id = yarn_info.pw_gid

    print("user:{0} start localizer  app_id:{1}, working_dir:{2}, local_dirs:{3}, log_dirs:{4}"
          .format(user_name, app_id, working_dir, local_dirs, log_dirs))

    # 这里本地化的时候，提前build image，imagename到时候重新指定
    docker = docker_utils.Docker(conf, user_name, uid)
    if not image_name:
      print("localizer is none, use image {0}".format(image_name))
      if docker.docker_registry:
        image_name = "{0}/{1}/{2}".format(docker.docker_registry, user_name, docker.default_localizer_image_name)
      else:
        image_name = "{0}/{1}".format(user_name, docker.default_localizer_image_name)
    image = docker.pull_image(image_name)
    if not image:
      print ("pull localizer_image_name {0} fail.".format(image_name))
      sys.exit(1)

    for _dir in local_dirs:
      if not os.path.exists("{0}/usercache".format(_dir)):
        create_dir("{0}/usercache".format(_dir), yarn_info.pw_uid, yarn_id, 0755)

    status = create_user_local_dirs(local_dirs, user_name, uid, yarn_id)
    if not status:
      return False
    status = create_user_cache_dirs(local_dirs, user_name, uid, yarn_id)
    if not status:
      return False
    status = create_application_dirs(local_dirs, app_id, user_name, uid, yarn_id)
    if not status:
      return False

    status = create_app_log_dirs(log_dirs, app_id, user_name, uid, yarn_id)
    if not status:
      return False

    token_dst = "{0}/{1}.tokens".format(working_dir, loc_id)
    try:
      copy(nm_private_container_tokens_path, token_dst, uid, yarn_id, 0644)
    except:
      print ("copy token from {0} to {1} fail".format(nm_private_container_tokens_path, token_dst),
             traceback.format_exc())
      return False
    os.chdir(working_dir)
    _type = conf.get("container-executor.account.type", "mos")
    if _type == "mos":
      docker_exec = conf.get("yarn-nodemanager.docker-executor.path", "docker")
      container_localizer_sh = write_mos_container_localizer(uid, yarn_id, container_working_dir=working_dir,
                                                             cmd_shell=cmd_shell)
      volumes.append(working_dir)
      volume = make_volume_str(volumes)
      docker_shell = "{0} run -u {1} --rm --net=host {2} {3} bash {4}".format(docker_exec, user_name, volume,
                                                                              image_name,
                                                                              container_localizer_sh)
      code = os.system(docker_shell)
    else:
      os.setuid(uid)
      code = os.system(cmd_shell)
    if code:
      print("localizer failed.")
      return False
    return True
  except:
    print(traceback.format_exc())
    return False


def launch_container(nm_private_container_script_path, nm_private_tokens_path, container_working_dir, app_id,
                     container_id, image_name, pid_path, user_name, cpu=8, memory=32768, gpus=[], volumes=[], conf={}):
  try:
    user_name, uid, group_name, group_id = get_user_info(user_name, conf)

    docker_exec = conf.get('yarn-nodemanager.docker-executor.path', None)
    if not docker_exec:
      print("yarn-nodemanager.docker-executor.path is not set.")
      sys.exit(1)

    black_users = conf.get("yarn.block.users", [])
    if user_name in black_users:
      print("{0} in yarn.block.users.".format(user_name))
      sys.exit(1)

    log_dirs = conf.get("yarn.nodemanager.log-dirs", [])
    if not log_dirs:
      print("yarn.nodemanager.log-dirs not set.")
      sys.exit(1)

    local_dirs = conf.get("yarn.nodemanager.local-dirs", [])
    if not local_dirs:
      print("yarn.nodemanager.local-dirs not set.")
      sys.exit(1)

    yarn_name = conf.get("yarn.nodemanager.docker-container-executor.group", "sankuai")

    print("user:{0} start localizer  app_id:{1}, container_id:{2}, container_working_dir:{3}, local_dirs:{4}, "
          "log_dirs:{5}".format(user_name, app_id, container_id, container_working_dir, local_dirs, log_dirs))

    yarn_info = pwd.getpwnam(yarn_name)
    yarn_id = yarn_info.pw_gid

    docker = docker_utils.Docker(conf, user_name, uid)
    image = docker.pull_image(image_name)
    if not image:
      print("pull launch_container_image_name {0} fail.".format(image_name))
      sys.exit(1)

    create_container_dirs(local_dirs, app_id, container_id, user_name, uid, yarn_id)

    create_container_log_dirs(log_dirs, app_id, container_id, user_name, uid, yarn_id)

    # create temp dir in working dir
    tmp_dir = "{0}/tmp".format(container_working_dir)
    create_dir(tmp_dir, uid, yarn_id, CONTAINER_TMP_PERM)

    # copy container token to working dir
    token_dst = "{0}/container_tokens".format(container_working_dir)
    try:
      copy(nm_private_tokens_path, token_dst, uid, yarn_id, 0600)
    except:
      print ("copy token from {0} to {1} fail".format(nm_private_tokens_path, token_dst),
             traceback.format_exc())

    # copy container lanuch script
    launch_dst = "{0}/launch_container.sh".format(container_working_dir)
    try:
      copy(nm_private_container_script_path, launch_dst, uid, yarn_id, 0700)
    except:
      print("copy launch_container.sh from {0} to {1} fail.".format(nm_private_container_script_path, launch_dst),
            traceback.format_exc())
      return False

    os.chdir(container_working_dir)
    write_docker_container_executor_session(user_name=user_name, uid=uid, gid=yarn_id, pid_path=pid_path,
                                            local_dirs=local_dirs, app_id=app_id,
                                            container_working_dir=container_working_dir, log_dirs=log_dirs,
                                            container_id=container_id, docker_exec=docker_exec,
                                            image_name=image_name, launch_sh=launch_dst, cpu=cpu, memory=memory,
                                            gpus=gpus, volumes=volumes, conf=conf)

    write_docker_container_executor(uid=uid, gid=yarn_info.pw_gid, container_working_dir=container_working_dir,
                                    pid_path=pid_path)
    return True
  except:
    print(traceback.format_exc())
    return False


def del_as_user(user_name, del_paths, conf={}):
  # user_name, uid, group_name, group_id = get_user_info(user_name, conf)
  # os.setuid(uid)
  print("user:{0} del del_paths:{1}".format(user_name, del_paths))
  all_delete = True
  for del_path in del_paths:
    if not os.path.exists(del_path):
      continue
    try:
      if os.path.isfile(del_path):
        os.remove(del_path)
      else:
        shutil.rmtree(del_path)
    except:
      print ("delete file {0} fail.\n{1}".format(del_path, traceback.format_exc()))
      all_delete = not os.path.exists(del_path)
  if not all_delete:
    sys.exit(1)


def rename_local_file_for_cleanup(conf, source_path, dest_path):
  yarn_name = conf.get("yarn.nodemanager.docker-container-executor.group", "sankuai")
  yarn_info = pwd.getpwnam(yarn_name)

  uid = yarn_info.pw_uid
  gid = yarn_info.pw_gid

  chown_path(source_path, uid, gid)
  try:
    os.rename(source_path, dest_path)
    return True
  except:
    print("rename {0} to {1} fail.\n".format(source_path, dest_path, traceback.format_exc()))
    return False


def signal_container(pid, signal, conf):
  if signal == SIGKILL:
    docker_utils.Docker(conf).stop_container_by_pid(pid)
  else:
    code = os.system("kill -{0} {1}".format(signal, pid))
    if code:
      sys.exit(code)


def wait_container(conf, user_name, container_id):
  print("wait {0} ".format(container_id))
  return os.system("docker wait {0}".format(container_id))


def clean_images_and_tmp(conf):
  print("begin clean images")
  docker = docker_utils.Docker(conf)
  docker.clean_images()
  users = docker.list_running_user()
  print("begin clean temp dir. {0} user is running, ignore".format(users))
  local_dirs = conf.get("yarn.nodemanager.local-dirs", [])

  for local_dir in local_dirs:
    user_cache = "{0}/{1}".format(local_dir, USERCACHE)
    dirs = os.listdir(user_cache)
    for user in users:
      if user in dirs:
        dirs.remove(user)
    for user in dirs:
      user_path = get_user_cache_dir(local_dir, user)
      sub_dirs = os.listdir(user_cache)
      ts = 0
      for _dir in sub_dirs:
        _path = "{0}/{1}".format(user_cache, _dir)
        if os.path.getmtime(_path) > ts:
          ts = os.path.getmtime(_path)
          print(_path, ts)
      now_ts = time.time()
      t = conf.get("temp.dir.retention.time", 864000)
      if now_ts - ts > t:
        print("{0} last modification time is {1}, and no {2}'s job run it, can remove it.".format(user_path, ts, user))
        shutil.rmtree(user_path)


