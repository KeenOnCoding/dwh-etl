def call(status){

    color = "00FF00"
    if (currentBuild.currentResult != 'SUCCESS'){
        color = "FF0C00"
    }
    office365ConnectorSend (
            status: "**${status}**",
            webhookUrl: "${WEBHOOK_URL}",
            color: color,
            message: "Job name: ${JOB_NAME} - ${BUILD_DISPLAY_NAME}<br>Pipeline duration: ${currentBuild.durationString}"
    )
}