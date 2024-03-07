@Library('DPS-Jenkins-Lib') _

pipeline {
//This job is intended for tracking and storing records changes. It was created to help track changes in dimensions, but can be also used to track fact changes.
//
//To use this job, the records should contain a hash sum field, that is calculated over the other record fields, some identifier column that is constant, and a batch id column. This job watches the state of the staging table, tracks the changes and stores it in the historical table.
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
        WEBHOOK_URL = credentials('WEBHOOK_URL')
    }
    stages {
        stage('Track ETS Users Records Changes') {
            steps {
                runHistoricalCopy 'track-records-changes'
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