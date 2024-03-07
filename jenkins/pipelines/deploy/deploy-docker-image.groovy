pipeline {
    agent any
    options {
        skipDefaultCheckout()
    }
    stages {
        stage('Build Docker Image') {
            steps {
                cleanWs()
                dir('dwh-etl') {
                    checkout scm
                }
                sh 'docker version'
                echo 'Available Docker Images:'
                sh 'docker images'
                sh "docker build -f dwh-etl/Dockerfile -t dwh:${GLOBAL_DOCKER_IMAGE_TAG} dwh-etl/"
                sh 'docker system prune -f'
            }
        }
    }
}

