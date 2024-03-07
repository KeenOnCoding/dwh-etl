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

        CLIENT_ID = credentials('API_MANAGER_CLIENT_ID')
        CLIENT_SECRET = credentials('API_MANAGER_CLIENT_SECRET')

        WEBHOOK_URL = credentials('WEBHOOK_URL')
    }
    stages {
        stage('Employee Work History Ingestion and Processing') {
            steps {
                script {
                    jobs = [
                            ResonExtendedLeave       : 'reasons-extended-leave',
                            EmployeeWorkHistory      : 'employee-work-history',
                            EmployeeHomeHistory      : 'employees-home-history',
                            EmployeeTransferDismissal: 'employees-transfer-dismissal'

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