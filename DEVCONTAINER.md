# Devcontainer

## Coredumps
When postgres/citus crashes, there is the option to create a coredump. This is useful for debugging the issue. Coredumps are enabled in the devcontainer by default. However, not all environments are configured correctly out of the box. The most important configuration that is not standardized is the `core_pattern`. The configuration can be verified from the container, however, you cannot change this setting from inside the container as the filesystem containing this setting is in read only mode while inside the container.

To verify if corefiles are written run the following command in a terminal. This shows the filename pattern with which the corefile will be written.
```bash
cat /proc/sys/kernel/core_pattern
```

This should be configured with a relative path or simply a simple filename, such as `core`. When your environment shows an absolute path you will need to change this setting. How to change this setting depends highly on the underlying system as the setting needs to be changed on the kernel of the host running the container.

You can put any pattern in `/proc/sys/kernel/core_pattern` as you see fit. eg. You can add the PID to the core pattern in one of two ways;
- You either include `%p` in the core_pattern. This gets substituted with the PID of the crashing process.
- Alternatively you could set `/proc/sys/kernel/core_uses_pid` to `1` in the same way as you set `core_pattern`. This will append the PID to the corefile if `%p` is not explicitly contained in the core_pattern.

When a coredump is written you can use the debug/launch configuration `Open core file` which is preconfigured in the devcontainer. This will open a fileprompt that lists all coredumps that are found in your workspace. When you want to debug coredumps from `citus_dev` that are run in your `/data` directory, you can add the data directory to your workspace. In the command pallet of vscode you can run `>Workspace: Add Folder to Workspace...` and select the `/data` directory. This will allow you to open the coredumps from the `/data` directory in the `Open core file` debug configuration.

### Windows (docker desktop)
When running in docker desktop on windows you will most likely need to change this setting. The linux guest in WSL2 that runs your container is the `docker-desktop` environment. The easiest way to get onto the host, where you can change this setting, is to open a powershell window and verify you have the docker-desktop environment listed.

```powershell
wsl --list
```

Among others this should list both `docker-desktop` and `docker-desktop-data`. You can then open a shell in the `docker-desktop` environment.

```powershell
wsl -d docker-desktop
```

Inside this shell you can verify that you have the right environment by running

```bash
cat /proc/sys/kernel/core_pattern
```

This should show the same configuration as the one you see inside the devcontainer. You can then change the setting by running the following command.
This will change the setting for the current session. If you want to make the change permanent you will need to add this to a startup script.

```bash
echo "core" > /proc/sys/kernel/core_pattern
```
