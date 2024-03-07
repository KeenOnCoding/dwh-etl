@Library('DPS-Jenkins-Lib') _

pipeline {
    agent any
    triggers {
        cron('H * * * *')
    }
    options {
        disableConcurrentBuilds()
        timeout(time: 15)
    }
    environment {
        DATABASE_PASS = credentials('DATABASE_PASS')
        API_KEY = credentials('API_MANAGER_KEY')
        API_USER = credentials('UTTOOL_USER')
        API_PASSWORD = credentials('UTTOOL_PASSWORD')

        WEBHOOK_URL = credentials('WEBHOOK_URL')
    }
    stages {
        stage('Utilization Tool Ingestion And Processing') {
            steps {
                script {
                    jobs = [
                            UtilizationTool   : 'uttool',
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
        stage('Check Errors') {
            steps{
                runCountCheck 'count-check'
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