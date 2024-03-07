@Library('DPS-Jenkins-Lib') _

pipeline {
    agent any
    triggers {
        cron('H 0 * * *')
    }
    options {
        disableConcurrentBuilds()
        timeout(time: 30)
    }
    environment {
        DATABASE_PASS = credentials('DATABASE_PASS')
        API_KEY = credentials('API_MANAGER_KEY')
        EXTRA_PARAMS = credentials('1C_FIN_TOKEN')

        CLIENT_ID = credentials('API_MANAGER_CLIENT_ID')
        CLIENT_SECRET = credentials('API_MANAGER_CLIENT_SECRET')

        WEBHOOK_URL = credentials('WEBHOOK_URL')
    }
    stages {
        stage('GM Extended Summary Ingestion And Processing') {
            steps {
                script {
                    jobs = [
                            GMExtendedSummary:          'gm-extended-summary',
                            GMExtendedSummaryTotals:    'gm-extended-summary-totals',
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