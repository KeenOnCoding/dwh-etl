@Library('DPS-Jenkins-Lib') _

pipeline {
    agent any
    triggers {
        cron('H H * * 1')
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
        stage('Work Experience') {
            steps {
                runWorkExperienceProcessor 'work-experience'
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