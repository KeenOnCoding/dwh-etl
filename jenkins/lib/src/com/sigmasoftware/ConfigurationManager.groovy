package com.sigmasoftware

import java.text.SimpleDateFormat
import java.util.Random


class ConfigurationManager {
    String directoryPath

    String generateTmpDirectoryPath(configurationName){
        String currentTime = getCurrentTime()
        int randomNumber = Math.abs(new Random().nextInt() % 1000)

        return "${configurationName}-${currentTime}-${randomNumber}"
    }

    String getCurrentTime(){
        SimpleDateFormat timeFormat = new SimpleDateFormat("HH-mm-ss")
        return timeFormat.format(new Date())
    }

    String getCurrentDate(){
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd")
        return dateFormat.format(new Date())
    }

    void createNewDirectory(configurationName){
        String currentDate = getCurrentDate()
        String currentName = generateTmpDirectoryPath(configurationName)
        File directory = new File("/tmp/job_configs/${currentDate}/${currentName}")
        directory.mkdirs()
        directoryPath = directory.getAbsolutePath()
    }

    String getDirectoryPath(){
        return directoryPath
    }

    String writeToFile(fileName, fileContext){
        String filePath = "${directoryPath}/${fileName}"
        File file = new File(filePath)
        file.write(fileContext)
        return filePath
    }

    String buildReplaceEnvVarsCommand(tmpFilePath, targetFileName){
        String targetPath = "${directoryPath}/${targetFileName}"
        return  "envsubst < ${tmpFilePath} > ${targetPath}"
    }
}

