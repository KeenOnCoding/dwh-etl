@Library('DPS-Jenkins-Lib') _

pipeline {
    agent any
    triggers {
        cron('H/20 * * * *')
    }
    options {
        disableConcurrentBuilds()
        timeout(time: 10)
    }
    environment {
        DATABASE_PASS = credentials('DATABASE_PASS')
        WEBHOOK_URL = credentials('WEBHOOK_URL')
    }
    stages {
        stage('Check Data Lag') {
            steps {
                runDataLag [:]  // use empty map as pipelines forbid running custom steps without parameters
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

