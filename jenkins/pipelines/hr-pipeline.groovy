@Library('DPS-Jenkins-Lib') _

pipeline {
    agent any
    triggers {
        cron('H 6 1 * *')
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
        stage('HR Ingestion And Processing') {
            steps {
                script {
                    jobs = [
                            RecruitmentForm:    'recruitment-forms',
                            HRHeadcount:        'hr-headcount',
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