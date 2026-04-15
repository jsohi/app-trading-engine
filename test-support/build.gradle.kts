plugins {
    `java-library`
}

dependencies {
    api(project(":messages"))
    api(libs.agrona)
    api(libs.aeron.cluster)
    api(libs.junit.jupiter)
}
