# -*- coding:utf-8 -*-

import os
import traceback
import shutil
import yaml
import ctypes
import pwd
import sys
import hashlib
reload(sys)
sys.setdefaultencoding('utf8')

# Permissions for user dir. local.dir/usercache/$user
USER_PERM = ctypes.c_uint32(02750).value

# Permissions for user appcache dir. $local.dir/usercache/$user/appcache
APPCACHE_PERM = ctypes.c_uint32(02750).value

# Permissions for user filecache dir. $local.dir/usercache/$user/filecache
FILECACHE_PERM = 0710

# Permissions for user app dir. $local.dir/usercache/$user/appcache/$appId
APPDIR_PERM = ctypes.c_uint32(02750).value

# Permissions for user app dir. $local.dir/usercache/$user/appcache/$appId/$containerId
CONTAINER_PERM = ctypes.c_uint32(02750).value

# Permissions for user app dir. $local.dir/usercache/$user/appcache/$appId/$containerId/tmp
CONTAINER_TMP_PERM = ctypes.c_uint32(02750).value

# Permissions for user log dir. $logdir/$user/$appId
LOGDIR_PERM = 0770

FILECACHE = "filecache"
APPCACHE = "appcache"
USERCACHE = "usercache"


def copy(source, dest, uid, gid, mode):
  shutil.copy(source, dest)
  os.chmod(dest, mode)
  os.chown(dest, uid, gid)


def create_user_local_dirs(local_dirs, user_name, uid, gid):
  """
  Initialize the local directories for a particular user.
  mkdir $local.dir/usercache/$user 
  :param local_dirs: 
  :param user_name: 
  :param uid: 
  :param gid: 
  :return: 
  """
  user_dir_status = False
  for _dir in local_dirs:
    status = create_dir(get_user_cache_dir(_dir, user_name), uid, gid, USER_PERM)
    if status:
      user_dir_status = True

  if not user_dir_status:
    print("Not able to initialize user directories in any of the configured local directories for user {0}"
          .format(user_name))
    return False
  return True


def create_user_cache_dirs(local_dirs, user_name, uid, gid):
  """
  Initialize the local cache directories for a particular user.
  local.dir/usercache/$user
  $local.dir/usercache/$user/appcache
  $local.dir/usercache/$user/filecache
  :param local_dirs: 
  :param user_name: 
  :param uid: 
  :param gid: 
  :return: 
  """
  appcache_dir_status = False
  distributed_cache_dir_status = False
  for _dir in local_dirs:
    app_cache_dir = get_app_cache_dir(_dir, user_name)
    status = create_dir(app_cache_dir, uid, gid, APPCACHE_PERM)
    if not status:
      print("Unable to create app cache directory : {0}".format(app_cache_dir))
    else:
      appcache_dir_status = True

  for _dir in local_dirs:
    file_dir = get_file_cache_dir(_dir, user_name)
    status = create_dir(file_dir, uid, gid, FILECACHE_PERM)
    if not status:
      print("Unable to create file cache directory : {0}".format(file_dir))
    else:
      distributed_cache_dir_status = True

  if not appcache_dir_status:
    print("Not able to initialize app-cache directories in any of the configured local directories for user {0}"
          .format(user_name))
    return False
  if not distributed_cache_dir_status:
    print("Not able to initialize distributed-cache directories in any of the configured local directories for user "
          "{0}".format(user_name))
    return False
  return True


def create_application_dirs(local_dirs, app_id, user_name, uid, gid):
  """
  Initialize the local directories for a particular user.
  $local.dir/usercache/$user/appcache/$appid
  :param local_dirs: 
  :param app_id: 
  :param user_name: 
  :param uid: 
  :param gid: 
  :return: 
  """
  init_app_dir_status = False
  for _dir in local_dirs:
    app_dir = get_application_dir(_dir, user_name, app_id)
    status = create_dir(app_dir, uid, gid, APPDIR_PERM)
    if not status:
      print("init app dir fail : {0}".format(app_dir))
    else:
      init_app_dir_status = True

  if not init_app_dir_status:
    print("Not able to initialize app directories in any of the configured local directories for app {0}"
          .format(app_id))
    return False
  return True


def create_container_dirs(local_dirs, app_id, container_id, user_name, uid, gid):
  """
  Initialize the local directories for a particular user.
  $local.dir/usercache/$user/appcache/$appid
  :param local_dirs: 
  :param app_id: 
  :param container_id
  :param user_name: 
  :param uid: 
  :param gid: 
  :return: 
  """
  init_container_dir_status = False
  for _dir in local_dirs:
    container_dir = get_container_dir(_dir, user_name, app_id, container_id)
    status = create_dir(container_dir, uid, gid, CONTAINER_PERM)
    create_dir("{0}/{1}_mos_work_dir", uid, gid, 0775)
    create_dir("{0}/{1}_tmp", uid, gid, 0777)
    if not status:
      print("init container dir fail : {0}".format(container_dir))
    else:
      init_container_dir_status = True

  if not init_container_dir_status:
    print("Not able to initialize container directories in any of the configured local directories for container_id "
          "{0}".format(app_id))
    return False
  return True


def create_app_log_dirs(log_dirs, app_id, user_name, uid, gid):
  app_log_dir_status = False
  for _dir in log_dirs:
    app_log_dir = "{0}/{1}".format(_dir, app_id)
    status = create_dir(app_log_dir, uid, gid, LOGDIR_PERM)
    if not status:
      print("Unable to create the container-log directory : {0}".format(app_log_dir))
    else:
      app_log_dir_status = True
  if not app_log_dir_status:
    print("Not able to initialize app-log directories in any of the configured local directories for app {0}"
          .format(app_id))
    return False
  return True


def create_container_log_dirs(log_dirs, app_id, container_id, user_name, uid, gid):
  container_log_dir_status = False
  for _dir in log_dirs:
    container_log_dir = "{0}/{1}/{2}".format(_dir, app_id, container_id)
    status = create_dir(container_log_dir, uid, gid, LOGDIR_PERM)
    if not status:
      print("Unable to create the container-log directory : {0}".format(container_log_dir))
    else:
      container_log_dir_status = True
  if not container_log_dir_status:
    print("Not able to initialize container-log directories in any of the configured local directories for container"
          " {0}".format(container_id))
    return False
  return True


def create_dir(_path, uid, gid, mode):
  print("create dir:{0}, uid:{1}, gid:{2}, mode:{3}.".format(_path, uid, gid, mode))
  try:
    parent_dir = os.path.abspath(os.path.normpath(_path) + os.path.sep + "..")
    if not os.path.exists(parent_dir):
      create_dir(parent_dir, uid, gid, mode)
    if not os.path.exists(_path):
      os.mkdir(_path, mode)
    os.chown(_path, uid, gid)
    os.chmod(_path, mode)
    return True
  except:
    print (traceback.format_exc())
    return False


def get_user_cache_dir(base, user_name):
  """Initialize the local directories for a particular user.
  :param base: $local.dir
  :param user_name: 用户名
  :return: $local.dir/usercache/$user
  """
  return "{0}/{1}/{2}/".format(base, USERCACHE, user_name)


def get_app_cache_dir(base, user_name):
  """Initialize the local directories for a particular user.
  :param base: $local.dir
  :param user_name: 用户名
  :return: $local.dir/usercache/$user/appcache
  """
  return "{0}/{1}/{2}/{3}/".format(base, USERCACHE, user_name, APPCACHE)


def get_file_cache_dir(base, user_name):
  """Initialize the local directories for a particular user.
  :param base: $local.dir
  :param user_name: 用户名
  :return: $local.dir/usercache/$user/filecache
  """
  return "{0}/{1}/{2}/{3}/".format(base, USERCACHE, user_name, FILECACHE)


def get_application_dir(base, user_name, app_id):
  """Initialize the local directories for a particular user.
  :param base: $local.dir
  :param user_name: 用户名
  :param app_id
  :return: $local.dir/usercache/$user/appcache/${app_id}
  """
  return "{0}/{1}/{2}/{3}/{4}/".format(base, USERCACHE, user_name, APPCACHE, app_id)


def get_container_dir(base, user_name, app_id, container_id):
  """Initialize the local directories for a particular user.
  :param base: $local.dir
  :param user_name: 用户名
  :param app_id
  :param container_id
  :return: $local.dir/usercache/$user/appcache/${app_id}/${container_id}
  """
  return "{0}/{1}/{2}/{3}/{4}/{5}".format(base, USERCACHE, user_name, APPCACHE, app_id, container_id)


docker_container_executor_session = "docker_container_executor_session.sh"
docker_container_executor = "docker_container_executor.sh"
get_docker_container_id = "get_docker_container_id.sh"


# 脚本yarn 的nm执行，都是有sankuai账号运行
def write_docker_container_executor_session(user_name, uid, gid, pid_path, container_working_dir, log_dirs, local_dirs,
                                            app_id, container_id, docker_exec, image_name, launch_sh, cpu=8,
                                            memory=32768, gpus=[], volumes=[], conf={}):
   session_script_path = '{0}/{1}'.format(container_working_dir, docker_container_executor_session)
   with open(session_script_path, 'w') as f:
    f.write("#!/usr/bin/env bash\n\n")
    # 获取docker 的pid
    # f.write("echo `{0} inspect --format {3} {1}` > {2}/{1}.tmp\n".format(docker_exec, container_id, pid_path,
    #                                                                      "{{.State.Pid}}"))
    # f.write('/bin/mv -f "{0}.tmp" "{0}"\n\n'.format(pid_path))
    # f.write("{0}\n  {1}\n{2} & \n\n".format("{", get_container_id_script_path, "}"))
    f.write("{\n")
    f.write("   flag=0\n")
    f.write("   while [[ $flag -lt 10 ]]\n")
    f.write("   do\n")
    f.write("       {0} inspect --format {1} {2}\n".format(docker_exec, "{{.State.Pid}}", container_id))
    f.write("       rc=$?\n")
    f.write("       if [[ $rc == 0 ]] ; then\n")
    f.write("         echo `{0} inspect --format {1} {2}` > {3}.tmp\n".format(docker_exec, "{{.State.Pid}}",
                                                                              container_id, pid_path))
    f.write('         /bin/mv -f {0}.tmp {0}\n'.format(pid_path))
    f.write("         flag=100\n")
    f.write("       fi\n")
    f.write("       sleep 2\n")
    f.write('       let "flag++"\n')
    f.write("   done\n")
    f.write("}&\n")
    
    device = ""
    if gpus:
      if conf.get("docker.exec.type", "docker") == "docker":
        device = " --device /dev/nvidiactl:/dev/nvidiactl --device /dev/nvidia-uvm:/dev/nvidia-uvm "
        index = 0
        for gpu in gpus:
          device = "{0} --device /dev/nvidia{1}:/dev/nvidia{2}".format(device, gpu, index)
          index = index + 1
      elif conf.get("docker.exec.type", "docker") == "nvidia-docker":
        for gpu in gpus:
          device = "{0} -mos-g {1}".format(device, gpu)
      else:
        print("un support docker.exec.type:", conf.get("docker.exec.type", None))
        sys.exit(1)
          
    control = ""
    if conf.get("docker.control.cpu", False):
      cpu_period = conf.get("docker.cpu.period", 100000)
      control = "{0} --cpu-period {1} --cpu-quota {2}".format(control, cpu_period, cpu_period * int(cpu))
    if conf.get("docker.control.memory", False):
      control = "{0} --memory {1}m --memory-swap -1".format(control, memory)

    ro_volumes = set(volumes)
    wr_volumes = set()
    wr_volumes.add(container_working_dir)
    if container_working_dir in ro_volumes:
      ro_volumes.remove(container_working_dir)
    for log_dir in log_dirs:
      _dir = "{0}/{1}/{2}".format(log_dir, app_id, container_id)
      wr_volumes.add(_dir)
      if _dir in ro_volumes:
        volumes.remove(_dir)
    for local_dir in local_dirs:
      user_cache = "{0}/usercache/{1}".format(local_dir, user_name)
      wr_volumes.add(user_cache)
      if user_cache in ro_volumes:
        ro_volumes.remove(user_name)

      file_cache = "{0}/filecache".format(local_dir)
      wr_volumes.add(file_cache)

      nm_private = "{0}/nmPrivate".format(local_dir)
      ro_volumes.add(nm_private)

    volume = "{0} {1}".format(make_volume_str(ro_volumes, ":ro"), make_volume_str(wr_volumes))
    volume = "{0} -v {1}/{2}_tmp:/tmp".format(volume, container_working_dir, container_id)
    volume = "{0} -v {1}/{2}_mos_work_dir:/mos_working_dir".format(volume, container_working_dir, container_id)

    f.write("\n\nrc=0\n")
    f.write("{\n")
    f.write("  {0} run --rm --net=host --user {1} --name {2} {3} {4} {5} {6} bash {7}\n".
            format(docker_exec, user_name, container_id, volume, device, control, image_name, launch_sh))
    f.write("rc=$?\n")
    f.write("}\n")
    f.write("exit $rc\n")
    os.chown(session_script_path, uid, gid)
    os.chmod(session_script_path, 0750)


def write_docker_container_executor(uid, gid, container_working_dir, pid_path):
  docker_container_executor_path = "{0}/{1}".format(container_working_dir, docker_container_executor)
  session_script_path = "{0}/{1}".format(container_working_dir, docker_container_executor_session)
  with open(docker_container_executor_path, "w") as f:
    f.write("#!/usr/bin/env bash\n\n")
    f.write('{0}\n'.format(session_script_path))
    f.write('rc=$?\n')
    f.write('echo $rc > {0}.exitcode.tmp\n'.format(pid_path))
    f.write('/bin/mv -f {0}.exitcode.tmp {0}.exitcode\n\n'.format(pid_path))
    f.write('exit $rc\n')
  os.chown(docker_container_executor_path, uid, gid)
  os.chmod(docker_container_executor_path, 0750)


def check_conf_file(_path):
  if not _path:
    print("config path is None.")
    return False
  if not os.path.exists(_path):
    print ("config path:{0} is not exists.".format(_path))
    return False
  if os.path.isdir(_path):
    print("config path:{0} is dir.".format(_path))
    return False
  return check_permission(_path)


def check_permission(_path):
  if not _path:
    print("path is None.")
    return False
  if not os.path.exists(_path):
    print("path :{0} is not exists.".format(_path))
    return False
  if len(_path) == 1 and '/' == _path:
    return True
  stat = os.stat(_path)
  uid = stat.st_uid
  gid = stat.st_gid
  mode = int(oct(stat.st_mode)) % 1000

  if uid != 0:
    print("{0} owner is not root".format(_path))
    return False
  if not gid:
    if mode % 100/10 not in [1, 4, 5]:
      print("{0} group permission is not 1、4 or 5. mode:{1}".format(_path, mode))
      return False

  if mode % 10 not in [1, 4, 5]:
    print("{0} other permission is not 1、4 or 5. mode:{1}".format(_path, mode))
    return False
  if check_permission(os.path.dirname(_path)):
    return True
  else:
    return False
  return True


def chown_path(_path, uid, gid):
  try:
    if not os.path.exists(_path):
      return
    os.chown(_path, uid, gid)
    os.chmod(_path, 0770)
    if os.path.isdir(_path):
      for _file in os.listdir(_path):
        chown_path("{0}/{1}".format(_path, _file), uid, gid)
  except:
    print("set file:{0} owner is fail:{1}".format(_path, traceback.format_exc()))
    return False


def load_config(_path):
  import sys
  reload(sys)
  sys.setdefaultencoding('utf8')
  if not check_conf_file(_path):
    return None
  stream = file(_path, 'r')
  return yaml.load(stream)


def user_name_2_uid_by_md5(user_name):
  md5 = hashlib.md5(user_name.encode("utf-8")).hexdigest()
  uid = 0
  index = 1
  for i in range(0, len(md5), 4):
    uid = ord(md5[i]) * index + ord(md5[i + 1]) * (index + 1) + ord(md5[i + 2]) * (index + 2) + ord(md5[i + 3]) * (index + 3) + uid
    index = index * 8
  return uid


def write_mos_container_localizer(uid, gid, cmd_shell, container_working_dir):
  container_localizer_sh = "{0}/localizer_container.sh".format(container_working_dir)
  with open(container_localizer_sh, "w") as f:
    f.write("#!/usr/bin/env bash\n\n")
    f.write("cd {0}\n\n".format(container_working_dir))
    f.write(cmd_shell)
    f.write("\n\n")
  os.chown(container_localizer_sh, uid, gid)
  os.chmod(container_localizer_sh, 0750)
  return container_localizer_sh


def get_user_info(user_name, conf):
  _type = conf.get("container-executor.account.type", "linux")
  if _type == "mos":
    uid = user_name_2_uid_by_md5(user_name)
    group_name = conf.get("mos.container-executor.group.name", "mos")
    group_id = conf.get("mos.container-executor.group.id", 8888)
    return user_name, uid, group_name, group_id
  elif _type == "linux":
    black_users = conf.get("yarn.block.users", [])
    if user_name in black_users:
      print("{0} in yarn.block.users.".format(user_name))
      sys.exit(1)
    max_uid = conf.get("yarn.max.user.id", sys.maxint)
    min_uid = conf.get("yarn.min.user.id", 0)
    try:
      user_info = pwd.getpwnam(user_name)
    except:
      print("Not get user:{0} info,{1}".format(user_name, traceback.format_exc()))
      sys.exit(1)
    uid = user_info.pw_uid
    if uid < min_uid or uid > max_uid:
      print("{0}'s uid is not betweeen yarn.max.user.id and yarn.min.user.id")
      sys.exit(1)
    group_name = conf.get("linux.container-executor.group.name", "mos")
    group_id = conf.get("linux.container-executor.group.id", 8888)
    return user_name, uid, group_name, group_id
  else:
    print("un support type {0}".format(_type))
    sys.exit(-1)

def make_volume_str(volumes, volume_type=""):
  abs_paths = map(lambda _path: os.path.abspath(_path), volumes)
  abs_paths = sorted(abs_paths)
  volume_paths = []
  for child in abs_paths:
    if not volume_paths:
      volume_paths.append(child)
    can_volume = True
    for parent in volume_paths:
      if is_child_dir(parent, child):
        can_volume = False
        break
    if can_volume:
      volume_paths.append(child)
  return " ".join(map(lambda _path: "-v {0}:{0}{1}".format(_path, volume_type), volume_paths))

def is_child_dir(parent, child):
  if not parent or not child:
    return False
  if len(os.path.abspath(child)) < len(os.path.abspath(parent)):
    return False
  if os.path.abspath(child) == os.path.abspath(parent):
                return True
  if os.path.dirname(child) == os.path.abspath(parent):
    return True
  return is_child_dir(parent, os.path.dirname(child))
