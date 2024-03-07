#!/bin/sh

PATH_TO_JAR="${PATH_TO_JAR:-"jenkins-cli.jar"}"
JENKINS_SERVER_URL="${JENKINS_SERVER_URL:-"http://vm-dps-05.ipa.dev.sigmaukraine.com:8080/"}"
JOB_FOLDER="${JOB_FOLDER:-"containers"}"

if [ ! -d "${JOB_FOLDER}" ]; then
  echo "No such folder ${JOB_FOLDER}"
  exit 1
fi

LOGIN="${LOGIN}"
PASSWORD="${PASSWORD}"

if [ -z "$LOGIN" ] || [ -z "$PASSWORD" ]; then
  echo "Wrong parameters for connection, LOGIN and PASSWORD environment variable must be set"
  exit 1
fi

echo "Jenkins jar file location ${PATH_TO_JAR}"
echo "Jenkins server uri ${JENKINS_SERVER_URL}"

CONNECTION_COMMAND="java -jar ${PATH_TO_JAR} -s ${JENKINS_SERVER_URL} -auth ${LOGIN}:${PASSWORD}"
GATHER_TOP_LEVEL="${GATHER_TOP_LEVEL:-1}"
JOB_FOLDERS="${JOB_FOLDERS:-"utility single-runnables deploy"}"

# list all jobs
TOP_LEVEL_CONFIGURATIONS=$(eval "${CONNECTION_COMMAND} list-jobs")

# for each job name
for top_level_conf_name in ${TOP_LEVEL_CONFIGURATIONS}; do

  # if job name is not a folder
  if ! (echo "${JOB_FOLDERS}" | grep -q "${top_level_conf_name}"); then
    # retrieve the job and store it under root directory
    if [ "$GATHER_TOP_LEVEL" -eq 1 ]; then
      job_xml_config=$(eval "${CONNECTION_COMMAND} get-job ${top_level_conf_name}")
      echo "${job_xml_config}" >"$(eval "pwd")/${JOB_FOLDER}/${top_level_conf_name}.xml"
      echo "${top_level_conf_name} job config was downloaded"
    fi
  else
    # else retrieve list of jobs located in the current folder
    job_folder=$top_level_conf_name
    folder_xml_config=$(eval "${CONNECTION_COMMAND} get-job ${job_folder}")
    echo "$folder_xml_config" >"$(eval "pwd")/${JOB_FOLDER}/${job_folder}/${job_folder}.xml"
    echo "${job_folder} folder config was downloaded"
    jobs_in_current_folder=$(eval "${CONNECTION_COMMAND} list-jobs ${job_folder}")

    # and for each name in the folder
    for job_name in ${jobs_in_current_folder}; do
      # retrieve job and store configuration under corresponding job folder
      job_xml_config=$(eval "${CONNECTION_COMMAND} get-job ${job_folder}/${job_name}")
      echo "$job_xml_config" >"$(eval "pwd")/${JOB_FOLDER}/${job_folder}/${job_name}.xml"
      echo "${job_name} job config was downloaded"
    done
  fi
done
