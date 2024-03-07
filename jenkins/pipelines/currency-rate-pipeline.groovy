@Library('DPS-Jenkins-Lib') _

pipeline {
    agent any
    triggers {
        cron('0 16 * * *')
    }
    options {
        disableConcurrentBuilds()
        timeout(time: 25)
    }
    environment {
        DATABASE_PASS = credentials('DATABASE_PASS')
        WEBHOOK_URL = credentials('WEBHOOK_URL')
    }
    stages {
        stage('Currency Rate Ingestion And Processing') {
            steps {
                script {
                    jobs = [
                            CurrencyRateNBU   : 'currency-rate-nbu',
                            RiksBank  : 'riksbank-currencies'
                    ]
                    jobs.each { jobName, configName ->
                        stage("${jobName} Ingestion") {
                            runIngestion "${configName}"
                        }
                        stage("${jobName} Processing") {
                            runSqlProcessor "${configName}"
                        }
                    }
                    stage("RiksBankCrossrate Processing") {
                        runRiksBankProcessor "riksbank-crossrate"
                    }
                    stage("RiksBankCrossrate SQLProcessor") {
                        runSqlProcessor "riksbank-crossrate"
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