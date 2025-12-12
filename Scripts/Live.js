onSnapshot(doc(db, "liveStreams", streamId), async snap => {
    if (!snap.exists()) {
        player.renderOffline();
        return;
    }

    const data = snap.data();

    if (!data.isLive) {
        player.renderOffline();
        return;
    }

    if (!playerLoaded) {
        await player.load({
            playbackUrl: data.playbackUrl,
            visibility: data.visibility,
            channelArn: data.channelArn,
        });
        playerLoaded = true;
    }
});
