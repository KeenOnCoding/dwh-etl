@Library('DPS-Jenkins-Lib') _

pipeline {
    agent any
    triggers {
        cron('H H 1,15 * *')
    }
    options {
        disableConcurrentBuilds()
        timeout(time: 60)
    }
    environment {
        DATABASE_PASS = credentials('DATABASE_PASS')
        API_KEY = credentials('API_MANAGER_KEY')

        CLIENT_ID = credentials('API_MANAGER_V2_CLIENT_ID')
        CLIENT_SECRET = credentials('API_MANAGER_V2_CLIENT_SECRET')

        WEBHOOK_URL = credentials('WEBHOOK_URL')
    }
    stages {
        stage('Calendars Ingestion And Processing') {
            steps {
                script {
                    jobs = [
                            Calendars     : 'calendars'
                    ]
                    jobs.each { jobName, configName ->
                        stage("${jobName} Ingestion") {
                            runIngestion "${configName}"
                        }
                        stage("${jobName} Processing") {
                            runSqlProcessor "${configName}"
                        }
                    }
                }
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