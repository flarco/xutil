export PROFILE_YAML=~/profile.yaml

# Useful alias
alias psu="ps aux | grep $USER"
alias pse="ps -eo pid,comm,cmd,start,etime"
alias py="python"
alias xutil-update='pip install -U git+https://github.com/flarco/xutil.git'

# Circus
alias tail-circus='tail -f /tmp/logs_circus/*.log'
alias cctl=circusctl
alias cstart='circusd ~/circus.ini'
alias crest='cctl restart'
alias cstop='cctl stop'
alias cquit='cctl quit'
alias creload='cctl reloadconfig'

# HDFS
alias dfs='hdfs dfs'
alias dfs-ls='dfs -ls'
alias dfs-rm='dfs -rm -f -skipTrash'
alias dfs-rmdir='dfs -rm -r -f -skipTrash'
alias dfs-rmtrash='dfs-rmdir /user/$USER/.Trash'
alias dfs-du='dfs -du -h'

cpobj() {
  # Copy file or folder to same path in another server
  # cpobj tgt_host /path/to/file/or/folder

  tgt_server=$1
  file_path=$2

  file_name="$(basename "$file_path")"
  parent_dir="$(dirname -- "$(readlink -f -- "$file_path")")"

  if [[ -d $file_path ]]; then
    # echo "file_path is a directory"
    scp -r $file_path $USER@$tgt_server:$parent_dir
  elif [[ -f $file_path ]]; then
    # echo "file_path is a file"
    scp -r $file_path $USER@$tgt_server:$parent_dir/$file_name
  else
    echo "$file_path is not valid"
    # exit 1
  fi
}