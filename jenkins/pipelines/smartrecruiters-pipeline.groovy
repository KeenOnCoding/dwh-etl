@Library('DPS-Jenkins-Lib') _


pipeline {
    agent any
    triggers {
        cron('H 5 * * *')
    }
    options {
        disableConcurrentBuilds()
        timeout(time: 20)
    }
    environment {
        DATABASE_PASS = credentials('DATABASE_PASS')
        API_KEY = credentials('API_MANAGER_KEY')

        CLIENT_ID = credentials('SR_CLIENT_ID')
        CLIENT_SECRET = credentials('SR_CLIENT_SECRET')

        WEBHOOK_URL = credentials('WEBHOOK_URL')
    }
    stages {
        stage('SmartRecruiters Ingestion And Processing') {
            steps {
                script {
                    jobs = [
                            SRPositions     :'sr-positions',
                            SRJobs          :'sr-jobs',
                            SRApplications  :'sr-applications',
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