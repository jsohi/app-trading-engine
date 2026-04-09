dependencies {
    implementation(project(":messages"))
    implementation(libs.agrona)
    implementation(libs.aeron.client)
    implementation(libs.snakeyaml)
}
