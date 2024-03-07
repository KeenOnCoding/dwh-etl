@Library('DPS-Jenkins-Lib') _

pipeline {
    agent any
    triggers {
        cron('H H * * *')
    }
    options {
        disableConcurrentBuilds()
        timeout(time: 30)
    }
    environment {
        DATABASE_PASS = credentials('DATABASE_PASS')
        WEBHOOK_URL = credentials('WEBHOOK_URL')
    }
    stages {
         stage('Daily Copy') {
             steps {
                 runDailyProcessor 'time-reports-daily-v2'
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