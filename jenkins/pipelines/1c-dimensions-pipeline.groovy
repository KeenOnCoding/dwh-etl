@Library('DPS-Jenkins-Lib') _

pipeline {
    agent any
    triggers {
        cron('H * * * *')
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

        CLIENT_ID_V2 = credentials('API_MANAGER_V2_CLIENT_ID')
        CLIENT_SECRET_V2 = credentials('API_MANAGER_V2_CLIENT_SECRET')

        WEBHOOK_URL = credentials('WEBHOOK_URL')
    }
    stages {
        stage('Dimension Ingestion And Processing') {
            steps {
                script {
                    jobs = [
                            Employee             : 'employee',
                            EmployeesV2          : 'employees-v2',
                            Project              : 'project',
                            Department           : 'department',
                            OrganizationalUnits  : 'organizational-units',
                            Category             : 'category',
                            DepartmentHistory    : 'department-history',
                            CategoryHistory      : 'category-history',
                            Position             : 'position',
                            PositionHistory      : 'position-history',
                            Account              : 'account',
                            Localization         : 'localization',
                            CostCenter           : 'cost-center',
                            Country              : 'country',
                            Domain               : 'domain',
                            Technology           : 'technology',
                            OpportunityCostCenter: 'opportunity-cost-center',
                            OpportunityAccount   : 'opportunity-account',
                            OpportunityProject   : 'opportunity-project',
                            City                 : 'city'
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