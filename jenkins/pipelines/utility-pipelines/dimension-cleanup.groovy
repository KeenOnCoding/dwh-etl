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
        WEBHOOK_URL = credentials('WEBHOOK_URL')
    }
    stages {
        stage('Run Dimension Cleanup') {
            steps {
                script {
                    jobs = [
                        Employee                  : 'employee',
                        Project                   : 'project',
                        Department                : 'department',
                        Category                  : 'category',
                        DepartmentHistory         : 'department-history',
                        CategoryHistory           : 'category-history',
                        Position                  : 'position',
                        Account                   : 'account',
                        PositionHistory           : 'position-history',
                        Localization              : 'localization',
                        CostCenter                : 'cost-center',
                        Workplace                 : 'workplace',
                        WorkplaceHistory          : 'workplace-history',
                        EmployeeWorkHistory       : 'employee-work-history',
                        EtsUser                   : 'ets-user',
                        Country                   : 'country',
                        Domain                    : 'domain',
                        SrJob                     : 'sr-job',
                        Uttool                    : 'uttool',
                        CurrencyRate              : 'currency-rate',
                        FinanceReports            : 'finance-reports',
                        EmployeeSatisfactionTypes : 'hr-tool-employee-satisfaction-types',
                        LeaveReasons              : 'hr-tool-leave-reasons',
                        Levels                    : 'hr-tool-levels',
                        Mentors                   : 'hr-tool-mentors',
                        RecordTypes               : 'hr-tool-record-types',
                        Requirements              : 'hr-tool-requirements',
                        Roadmaps                  : 'hr-tool-roadmaps',
                        Skills                    : 'hr-tool-skills',
                        City                      : 'city',
                        OpportunityAccount        : 'opportunity-account',
                        OpportunityCostCenter     : 'opportunity-cost-center',
                        OpportunityProject        : 'opportunity-project',
                        ReasonExtendedLeave       : 'reasons-extended-leave',
                        Technology                : 'technology',
                        Bank                      : 'bank',
                        Cashbox                   : 'cashbox',
                        BankAccount               : 'bank-account',
                        SrApplication             : 'sr-application',
                        Calendars                 : 'calendars',
                        DepartmentHierarchy       : 'department-hierarchy',
                        NLMSReports               : 'nlms-reports'
                    ]
                    jobs.each { jobName, jobConfigName ->
                        stage("${jobName} Cleanup") {
                            runDimensionCleanup jobConfigName
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

