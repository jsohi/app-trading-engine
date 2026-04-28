@echo off
rem Windows entry point for scripts/dev-jwks-server.mjs.
rem Forwards all args to node.
node "%~dp0dev-jwks-server.mjs" %*
