@Library('DPS-Jenkins-Lib') _

pipeline {
    agent any
    triggers {
        cron('H H * * *')
    }
    options {
        disableConcurrentBuilds()
    }
    environment {
        WEBHOOK_URL = credentials('WEBHOOK_URL')
    }
    stages {
        stage('Cleanup expired configs from tmp directory') {
            steps {
                sh '''
                    search_dir="/tmp/job_configs"
                    time_to_expire=3
                    current_date=$(date +"%Y-%m-%d")
                    
                    for dir in "$search_dir"/*
                    do
                      date_dir="${dir##*/}"
                      echo Check directory: $date_dir
                      if [[ "$date_dir" =~ ^[0-9]{4}[-][0-9]{2}[-][0-9]{2}$ ]];
                      then
                        date_diff=$(( ($(date -d "$current_date UTC" +%s) - $(date -d "$date_dir UTC" +%s)) / (60*60*24) ))
                        if [ $date_diff -gt $time_to_expire ]
                        then
                          rm -r "$dir"
                          echo Removed directory $dir due to 3 day expiration.
                        else
                          echo The directory is still valid.
                        fi
                      fi
                    done
                '''
            }
        }
    }
    post {
        failure {
            notify("${currentBuild.currentResult}")
        }
        fixed {
            notify("${currentBuild.currentResult}")
        }
    }
}