dependencies {
    implementation(project(":messages"))
    implementation(libs.aeron.client)
    implementation(libs.agrona)
    implementation(libs.gflog.api)
    runtimeOnly(libs.gflog.core)
}
