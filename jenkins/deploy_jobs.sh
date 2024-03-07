PATH_TO_JAR="${PATH_TO_JAR:-"jenkins-cli.jar"}"
JENKINS_SERVER_URL="${JENKINS_SERVER_URL:-"http://vm-dps-05.ipa.dev.sigmaukraine.com:8080/"}"


LOGIN="${LOGIN}"
PASSWORD="${PASSWORD}"

JOB_FOLDER="${JOB_FOLDER:-"containers"}"

if [ ! -d "${JOB_FOLDER}" ]; then
  echo "No such folder ${JOB_FOLDER}"
  exit 1
fi

if [ -z "$LOGIN" ] || [ -z "$PASSWORD" ]; then
  echo "Wrong parameters for connection, LOGIN and PASSWORD environment variable must be set"
  exit 1
fi

echo "Jenkins jar file location ${PATH_TO_JAR}"
echo "Jenkins server uri ${JENKINS_SERVER_URL}"

CONNECTION_COMMAND="java -jar ${PATH_TO_JAR} -s ${JENKINS_SERVER_URL} -auth ${LOGIN}:${PASSWORD}"

create_or_update_job() {
  JOB_NAME=$1
  JOB_PATH=$2
  exists=$(eval "$CONNECTION_COMMAND get-job ${JOB_NAME}")
  if [ -z "$exists" ]; then
    eval "$CONNECTION_COMMAND create-job ${JOB_NAME} < $JOB_PATH"
    if [ $? != 0 ]; then
      echo "Failed to create job"
      exit 1
    fi
    echo "Created job $JOB_NAME"
  else
    eval "$CONNECTION_COMMAND update-job ${JOB_NAME} < $JOB_PATH"
    if [ $? != 0 ]; then
      echo "Failed to update job"
      exit 1
    fi
    echo "Updated job $JOB_NAME"
  fi
}

# for each item in root folder
for file in "${JOB_FOLDER}"/*; do
  # if item is a directory
  if [ -d "$file" ]; then

    # deploy jenkins directory config
    path_to_folder_content="${file}"
    jenkins_folder_name=$(basename "$path_to_folder_content")
    create_or_update_job "${jenkins_folder_name}" "${path_to_folder_content}/${jenkins_folder_name}.xml"

    # for each other job in the directory
    for job in "${path_to_folder_content}"/*; do

      # deploy job if not job name equal to jenkins folder name
      job_name=$(basename "$job" | cut -f1 -d'.')
      if [ "${jenkins_folder_name}" != "${job_name}" ]; then
        create_or_update_job "${jenkins_folder_name}/${job_name}" "${job}"
      fi
    done
  else
    # else deploy top level job
    job_name=$(basename "${file}" | cut -f1 -d'.')
    create_or_update_job "${job_name}" "${file}"
  fi
done
