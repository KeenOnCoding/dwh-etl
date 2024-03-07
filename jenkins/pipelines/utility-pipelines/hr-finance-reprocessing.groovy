@Library('DPS-Jenkins-Lib') _

pipeline {
    agent any
    parameters {
        string(name: 'DATE_FROM', defaultValue: '', description: 'Inclusive bound. Example: 2026-02-01')
    }
    triggers {
        cron('H 0 * * *')
    }
    options {
        disableConcurrentBuilds()
        timeout(time: 10)
    }
    environment {
        DATABASE_PASS = credentials('DATABASE_PASS')
        WEBHOOK_URL = credentials('WEBHOOK_URL')
    }
    stages {
        stage('Cleanup HR Finance Data') {
            steps {
                script {
                    if (DATE_FROM.isEmpty()) {
                            DATE_FROM = sh(
                                    script: 'date --date="$(date +"%Y-%m-%d") -1 month" +"%Y-%m-01"',
                                    returnStdout: true
                            ).trim()
                    }
                    SQL = """declare @batches table(batch_id bigint)
                            insert into @batches(batch_id)
                            (select distinct batch_id
                            from hr_l2.fact_recruiters_salary_staging
                            where [date]>='${DATE_FROM}')
                            delete from hr_l2.fact_recruiters_salary
                            where batch_id in (select batch_id from @batches)
                            delete from hr_l2.fact_recruiters_salary_staging
                            where batch_id in (select batch_id from @batches);"""
                }

                echo "${SQL}"
                sh 'docker run --rm --entrypoint /opt/mssql-tools/bin/sqlcmd  mcr.microsoft.com/mssql/server  -S $DWH_HOST -d $DWH_DB -U $DWH_USER -P $DATABASE_PASS ' + "-Q \"${SQL}\""
            }
        }
        stage('Run HR Finance Processing') {
            steps {
                build job: '../hr-finance-pipeline'
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