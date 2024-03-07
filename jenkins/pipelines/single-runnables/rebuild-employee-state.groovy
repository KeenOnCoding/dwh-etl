@Library('DPS-Jenkins-Lib') _

pipeline {
    agent any
    triggers {
        cron('H * * * *')
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
        stage('Rebuild Employee Processing') {
            steps {
                script {
                    jobs = [
                            ChangeStateHomeHistory        : 'employee-change-state-v2',
                            ChangeStateHomeHistoryTransfer: 'employee-change-state-transfer',
                            StateDailyHomeHistory         : 'employee-state-daily-v2',
                            StateDailyUnionTransfer       : 'employee-state-daily-union-transfer'
                    ]
                    jobs.each { jobName, configName ->
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