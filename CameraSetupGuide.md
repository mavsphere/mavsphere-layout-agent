# Camera Setup Guide

This guide explains how to identify the correct USB camera devices for the MavSphere Layout Agent and what to enter into the configuration UI.

The layout-agent can use one or more USB cameras connected to the host machine.

A common source of confusion is that Linux may create **more than one `/dev/video*` entry for a single physical camera**. For example, one camera may appear as both `/dev/video0` and `/dev/video1`, but only one of them is the real video capture stream. The other may be a metadata/control node and will not produce a usable image.

Use the steps below to find the real camera device paths before entering them into the config UI.

---

## 1. Install camera inspection tools

On Ubuntu, Raspberry Pi OS, or Debian:

```bash
sudo apt update
sudo apt install -y v4l-utils ffmpeg
```

---

## 2. List connected cameras

```bash
v4l2-ctl --list-devices
```

Example output:

```text
USB Camera: USB Camera (usb-0000:01:00.0-1.2):
    /dev/video0
    /dev/video1
    /dev/media0

USB2.0 Camera: USB2.0 Camera (usb-0000:01:00.0-1.4):
    /dev/video2
    /dev/video3
    /dev/media1
```

In this example, the likely real capture devices are usually:

```text
/dev/video0
/dev/video2
```

The paired entries, such as `/dev/video1` and `/dev/video3`, are often metadata/control devices and should normally **not** be selected.

---

## 3. Check which `/dev/video*` entries are real capture devices

Run this for each device shown by `v4l2-ctl --list-devices`:

```bash
v4l2-ctl -d /dev/video0 --all | grep -E "Device Caps|Video Capture|Metadata Capture"
v4l2-ctl -d /dev/video1 --all | grep -E "Device Caps|Video Capture|Metadata Capture"
v4l2-ctl -d /dev/video2 --all | grep -E "Device Caps|Video Capture|Metadata Capture"
v4l2-ctl -d /dev/video3 --all | grep -E "Device Caps|Video Capture|Metadata Capture"
```

Select devices that show `Video Capture`. Avoid devices that only show `Metadata Capture`.

You can also check supported video formats:

```bash
v4l2-ctl -d /dev/video0 --list-formats-ext
v4l2-ctl -d /dev/video2 --list-formats-ext
```

A real camera stream normally lists formats such as `MJPG`, `YUYV`, or similar. A metadata-only node may show no useful video formats.

---

## 4. Test the camera manually on the host

Use `ffmpeg` to confirm the selected device actually produces frames:

```bash
ffmpeg -f v4l2 -i /dev/video0 -frames:v 1 test-video0.jpg
ffmpeg -f v4l2 -i /dev/video2 -frames:v 1 test-video2.jpg
```

Then check that the images were created:

```bash
ls -lh test-video*.jpg
```

If one command creates an image and another hangs, errors, or creates a blank/invalid file, use the working device in the config UI.

---

## 5. Prefer stable camera paths when possible

The `/dev/video0`, `/dev/video2` numbering can change after reboot or after unplugging/replugging cameras. To see stable names, run:

```bash
ls -l /dev/v4l/by-id/
ls -l /dev/v4l/by-path/
```

Example:

```text
usb-046d_HD_Webcam_C270-video-index0 -> ../../video0
usb-046d_HD_Webcam_C270-video-index1 -> ../../video1
usb-Generic_USB2.0_Camera-video-index0 -> ../../video2
usb-Generic_USB2.0_Camera-video-index1 -> ../../video3
```

Prefer entries ending in `video-index0` for each physical camera. These are normally the real capture devices. Avoid `video-index1` unless testing proves it is the actual stream.

For example, these are likely good selections:

```text
/dev/v4l/by-id/usb-046d_HD_Webcam_C270-video-index0
/dev/v4l/by-id/usb-Generic_USB2.0_Camera-video-index0
```

---

## 6. Enter the selected devices into the config UI

In the layout-agent config UI, enter one real capture device per camera.

Examples:

```text
/dev/video0
/dev/video2
```

or, preferably, stable paths such as:

```text
/dev/v4l/by-id/usb-046d_HD_Webcam_C270-video-index0
/dev/v4l/by-id/usb-Generic_USB2.0_Camera-video-index0
```

If using JSON config directly, the camera section should use the same selected device paths. For example:

```json
"cameras": [
  {
    "id": "front",
    "name": "Front camera",
    "device": "/dev/video0"
  },
  {
    "id": "rear",
    "name": "Rear camera",
    "device": "/dev/video2"
  }
]
```

---

## 7. Ensure Docker can access the selected cameras

If the agent runs in Docker, the selected camera devices must be passed into the container.

For simple `/dev/video*` paths, the Docker Compose service should include the devices you selected:

```yaml
services:
  layout-agent:
    devices:
      - /dev/video0:/dev/video0
      - /dev/video2:/dev/video2
```

If you use stable `/dev/v4l/by-id/...` paths in the config, also mount `/dev/v4l` and the underlying `/dev/video*` devices. For example:

```yaml
services:
  layout-agent:
    devices:
      - /dev/video0:/dev/video0
      - /dev/video2:/dev/video2
    volumes:
      - /dev/v4l:/dev/v4l:ro
```

After changing Docker Compose, restart the agent:

```bash
cd deploy
docker compose down
docker compose up -d
```

---

## 8. Check cameras from inside the container

```bash
docker ps
docker exec -it <layout-agent-container-name> ls -l /dev/video*
docker exec -it <layout-agent-container-name> ls -l /dev/v4l/by-id/
```

If the camera path entered in the config UI does not exist inside the container, the agent will not be able to open it.

---

## Troubleshooting

| Symptom | Likely cause | Fix |
| --- | --- | --- |
| One camera works, the other does nothing | Wrong `/dev/video*` selected, often metadata node | Test with `v4l2-ctl` and select the `Video Capture` device |
| Camera works on host but not in Docker | Device not passed into container | Add it under `devices:` in Docker Compose |
| Camera worked before reboot but now fails | `/dev/video*` numbering changed | Use `/dev/v4l/by-id/...video-index0` stable paths |
| Both cameras show the same image | Same device path entered twice | Give each camera a different tested capture path |
| Agent logs show camera open failure | Device missing or busy | Check Docker device mapping and stop other apps using the camera |

---

## README reference snippet

Add this small note to the main `README.md` camera section:

```markdown
### Cameras (optional)

The layout-agent can use one or more USB cameras connected to the host machine.

Linux may expose more than one `/dev/video*` entry for a single physical camera. Some entries are metadata/control devices and will not produce video.

Before configuring cameras, see [CameraSetupGuide.md](./CameraSetupGuide.md) for commands to list, test, and select the correct camera devices.
```
