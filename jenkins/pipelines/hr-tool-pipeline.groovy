@Library('DPS-Jenkins-Lib') _

pipeline {
    agent any
    triggers {
        cron('H 0 * * *')
    }
    options {
        disableConcurrentBuilds()
        timeout(time: 60)
    }
    environment {
        DATABASE_PASS = credentials('DATABASE_PASS')
        API_KEY = credentials('API_MANAGER_KEY')

        CLIENT_ID = credentials('API_MANAGER_CLIENT_ID')
        CLIENT_SECRET = credentials('API_MANAGER_CLIENT_SECRET')

        QUESTIONNAIRE_SUBMISSION = 'hr-tool-questionnaire-submission'

        WEBHOOK_URL = credentials('WEBHOOK_URL')
    }

    stages {
        stage('HR Tool Dimension Ingestion & Processing') {
            steps {
                script {
                    jobs = [
                            EmployeeSatisfactionTypes: 'hr-tool-employee-satisfaction-types',
                            LeaveReasons             : 'hr-tool-leave-reasons',
                            Levels                   : 'hr-tool-levels',
                            Mentors                  : 'hr-tool-mentors',
                            RecordTypes              : 'hr-tool-record-types',
                            Requirements             : 'hr-tool-requirements',
                            Roadmaps                 : 'hr-tool-roadmaps',
                            Skills                   : 'hr-tool-skills',
                            SmartGoals               : 'hr-tool-smartgoals',
                            Questionnaire            : 'hr-tool-questionnaire',
                            QuestionnaireSubmission  : 'hr-tool-questionnaire-submission',
                            F2F                      : 'hr-tool-f2f',
                            Risks                    : 'hr-tool-risks',
                    ]
                    jobs.each { jobName, configName ->
                        stage("${jobName} Dimension Ingestion") {
                            runIngestion "${configName}"
                        }
                        stage("${jobName} Dimension Processing") {
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