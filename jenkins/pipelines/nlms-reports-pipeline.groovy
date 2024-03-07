@Library('DPS-Jenkins-Lib') _

pipeline {
    agent any
    triggers {
        cron('H 04 * * *')
    }
    options {
        disableConcurrentBuilds()
        timeout(time: 70)
    }
    environment {
        DATABASE_PASS = credentials('DATABASE_PASS')
        WEBHOOK_URL = credentials('WEBHOOK_URL')
        NLMS_KEY = credentials('NLMS_KEY')

    }
    stages {
        stage('NLMS Reports') {
            steps {
                runNLMSProcessor 'nlms-reports'
            }
        }
         stage('Run SQLProcessor') {
            steps {
                runSqlProcessor 'nlms-reports'
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