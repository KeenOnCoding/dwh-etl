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
        CLIENT_ID = credentials('API_MANAGER_CLIENT_ID')
        CLIENT_SECRET = credentials('API_MANAGER_CLIENT_SECRET')

    }
    stages {
        stage('Courses Tasks Ingestion And Processing') {
            steps {
                script {
                    jobs = [
                            CoursesTasks   : 'courses-tasks',
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