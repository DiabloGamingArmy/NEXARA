(() => {
  const messageEl = document.getElementById('not-found-message');
  const homeBtn = document.getElementById('not-found-home');

  if (homeBtn) {
    homeBtn.addEventListener('click', () => {
      window.location.href = '/home';
    });
  }

  const fallbackMessages = [
    "We've hit a dead end.",
    "Signal lost. Let's get you back.",
    "Our tracks have gone cold.",
    "This page took the backroads.",
    "That trail goes nowhere.",
    "We couldn't find what you were looking for.",
    "Nothing here but stardust.",
    "Looks like this page drifted off course.",
    "We took a wrong turn in the feed.",
    "The route ends here."
  ];

  function setMessage(messages) {
    if (!messageEl) return;
    const list = Array.isArray(messages) && messages.length ? messages : fallbackMessages;
    const random = list[Math.floor(Math.random() * list.length)];
    messageEl.textContent = random;
  }

  fetch('/assets/data/notFoundMessages.json', { cache: 'no-cache' })
    .then((response) => response.json())
    .then((data) => setMessage(data))
    .catch(() => setMessage());
})();
