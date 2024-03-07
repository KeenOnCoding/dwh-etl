@Library('DPS-Jenkins-Lib') _

pipeline {
    agent any
    triggers {
        cron('H */4 * * *')
    }
    options {
        disableConcurrentBuilds()
        timeout(time: 90)
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
        stage('Finance Ingestion And Processing') {
            steps {
                script {
                    jobs = [
                            Finance:                'finance',
                            OvrPerFTEMonthlyConf:   'ovr_per_fte_monthly',
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