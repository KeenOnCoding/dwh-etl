package com.sigmasoftware

class Utils {
    static String buildDockerCommand(command, params, dir, image, tag, subcommand) {
        if (command == null || image == null) {
            throw new RuntimeException('Docker command or image is not specified')
        }

        "docker ${command} ${params ?: ""} -v ${dir}:/config ${image}${tag ? ":$tag" : ""} ${subcommand ?: ""}"
    }
}
