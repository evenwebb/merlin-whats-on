    (function() {
      var filmsEl = document.getElementById('films');
      var storageKey = 'wtw-whats-on-cinema';
      if (!filmsEl) return;
      var initialN = parseInt(filmsEl.getAttribute('data-initial-showings') || '40', 10);
      var maxDays = parseInt(filmsEl.getAttribute('data-max-showtime-days') || '10', 10);
      if (initialN < 1) initialN = 1;
      if (!Number.isFinite(maxDays) || maxDays < 1) maxDays = 10;

      function normalizeRow(r) {
        if (Array.isArray(r)) {
          return {
            date: r[0] || '',
            time: r[1] || '',
            screen: r[2] || '',
            cinema_name: r[3] || '',
            booking_url: r[4] || '',
            tags: r[5] || [],
            sold_out: r.length > 6 ? !!r[6] : false
          };
        }
        return {
          date: r.date || '',
          time: r.time || '',
          screen: r.screen || '',
          cinema_name: r.cinema_name || '',
          booking_url: r.booking_url || '',
          tags: r.tags || [],
          sold_out: !!r.sold_out
        };
      }

      function parseShowtimesJson(text) {
        var o = JSON.parse(text);
        if (o && o.v === 1 && Array.isArray(o.r)) return o.r.map(normalizeRow);
        if (Array.isArray(o)) return o.map(normalizeRow);
        return [];
      }

      function rowMatches(row, selDate, selCinema, selSaver) {
        if (selDate !== 'all' && row.date !== selDate) return false;
        if (selCinema !== 'all' && row.cinema_name !== selCinema) return false;
        if (selSaver === 'super-saver') {
          var tags = row.tags || [];
          var hasSaver = false;
          for (var ti = 0; ti < tags.length; ti++) {
            if (tags[ti] === 'Super Saver') { hasSaver = true; break; }
          }
          if (!hasSaver) return false;
        }
        return true;
      }

      function sortRows(rows) {
        return rows.slice().sort(function(a, b) {
          if (a.date !== b.date) return a.date < b.date ? -1 : 1;
          if (a.time !== b.time) return a.time < b.time ? -1 : 1;
          if (a.screen !== b.screen) return a.screen < b.screen ? -1 : 1;
          return (a.booking_url || '').localeCompare(b.booking_url || '');
        });
      }

      function splitInitial(rows) {
        var display = [];
        var hidden = [];
        var keptDates = {};
        function countDates() {
          var n = 0;
          for (var k in keptDates) {
            if (Object.prototype.hasOwnProperty.call(keptDates, k)) n++;
          }
          return n;
        }
        for (var i = 0; i < rows.length; i++) {
          var st = rows[i];
          var d = st.date || '';
          if (!d) continue;
          if (!keptDates[d] && countDates() >= maxDays) {
            hidden.push(st);
            continue;
          }
          if (!keptDates[d]) keptDates[d] = true;
          if (display.length >= initialN) {
            hidden.push(st);
            continue;
          }
          display.push(st);
        }
        return { display: display, hidden: hidden };
      }

      function escapeHtml(s) {
        return String(s)
          .replace(/&/g, '&amp;')
          .replace(/</g, '&lt;')
          .replace(/>/g, '&gt;')
          .replace(/"/g, '&quot;');
      }

      function dayHeaderLabel(iso) {
        try {
          var d = new Date(iso + 'T12:00:00Z');
          return d.toLocaleDateString('en-GB', {
            timeZone: 'Europe/London',
            weekday: 'short',
            day: 'numeric',
            month: 'short'
          });
        } catch (e) {
          return iso;
        }
      }

      var tagIconIds = {
        'Audio Description': 'icon-audio-desc',
        'Wheelchair access': 'icon-wheelchair',
        '2D': 'icon-2d',
        '3D': 'icon-3d',
        'Subtitles': 'icon-subtitles',
        'Silver Screen': 'icon-silver-screen',
        'Event cinema': 'icon-event-cinema',
        'Advance Screening': 'icon-event-cinema',
        'Strobe Light warning': 'icon-strobe',
        'Parent & Baby': 'icon-parent-baby',
        'Autism Friendly': 'icon-autism-friendly',
        'Kids Club': 'icon-kids-club'
      };
      var tagShortLabels = {
        'Audio Description': 'AD',
        'Subtitles': 'Subs',
        'Wheelchair access': 'WA',
        'Strobe Light warning': 'Strobe',
        'Hard of Hearing': 'HOH',
        'Private Box': 'Box',
        'Super Saver': 'Saver'
      };
      var tagTooltips = {
        'Audio Description': 'Audio description',
        'Subtitles': 'Subtitled screening',
        'Wheelchair access': 'Wheelchair accessible',
        '2D': 'Standard 2D screening',
        'Strobe Light warning': 'Strobe lighting may affect photosensitive viewers',
        'Hard of Hearing': 'Infrared hard of hearing available'
      };

      function oneTagHtml(tag) {
        var iconId = tagIconIds[tag];
        var label = tagShortLabels[tag] || tag;
        var tooltip = tagTooltips[tag] || (tagShortLabels[tag] ? null : tag);
        var titleEsc = tooltip ? escapeHtml(tooltip) : '';
        var titleAttr = titleEsc ? ' title="' + titleEsc + '"' : '';
        if (iconId) {
          return '<span class="tag"' + titleAttr + '><svg class="tag-icon" aria-hidden="true"><use href="#' + iconId + '"/></svg>' + escapeHtml(label) + '</span>';
        }
        return '<span class="tag"' + titleAttr + '>' + escapeHtml(label) + '</span>';
      }

      function renderShowtimeRows(rows) {
        var byDate = {};
        for (var i = 0; i < rows.length; i++) {
          var st = rows[i];
          var d = st.date || '';
          if (!byDate[d]) byDate[d] = [];
          byDate[d].push(st);
        }
        var keys = Object.keys(byDate).sort();
        var parts = [];
        for (var ki = 0; ki < keys.length; ki++) {
          var d = keys[ki];
          var times = byDate[d];
          var timeParts = [];
          for (var j = 0; j < times.length; j++) {
            var st = times[j];
            var t = st.time || '';
            var screen = escapeHtml(st.screen || '');
            var booking = st.booking_url || '';
            var soldOut = st.sold_out;
            var tags = st.tags || [];
            var tagSpan = '';
            for (var ti = 0; ti < Math.min(tags.length, 4); ti++) {
              tagSpan += (ti ? ' ' : '') + oneTagHtml(tags[ti]);
            }
            var timeEl;
            if (booking && !soldOut) {
              timeEl = '<a href="' + escapeHtml(booking) + '">' + escapeHtml(t) + '</a>';
            } else if (soldOut) {
              timeEl = '<span class="past">' + escapeHtml(t) + ' Sold Out</span>';
            } else {
              timeEl = '<span class="past">' + escapeHtml(t) + '</span>';
            }
            timeParts.push(
              '<div class="st-row"><span class="st-time">' + timeEl + '</span><span class="st-screen">' + screen + '</span><span class="st-tags">' + tagSpan + '</span></div>'
            );
          }
          parts.push(
            '<div class="day-group"><div class="st-date">' + escapeHtml(dayHeaderLabel(d)) + '</div>' + timeParts.join('') + '</div>'
          );
        }
        return parts.join('\n');
      }

      function buildShowtimesInner(picked, optionsKey) {
        picked = sortRows(picked);
        var sp = splitInitial(picked);
        var mainHtml = renderShowtimeRows(sp.display);
        var hidden = sp.hidden;
        var n = hidden.length;
        if (n === 0) return mainHtml;
        var extraId = 'showtimes-extra-' + optionsKey;
        var moreLabel = 'Show ' + n + ' more showings';
        return (
          mainHtml +
          '<div class="showtimes-actions"><button type="button" class="showtimes-more-btn" data-target="' +
          escapeHtml(extraId) +
          '" data-more-label="' +
          escapeHtml(moreLabel) +
          '" data-less-label="' +
          escapeHtml('Show fewer showings') +
          '">' +
          escapeHtml(moreLabel) +
          '</button></div><div id="' +
          escapeHtml(extraId) +
          '" class="showtimes-extra" hidden>' +
          renderShowtimeRows(hidden) +
          '</div>'
        );
      }

      function applyFilters() {
        var dateBtn = document.querySelector('.tab-date.active');
        var cinemaBtn = document.querySelector('.tab-cinema.active');
        var saverBtn = document.querySelector('.tab-saver.active');
        var selDate = dateBtn ? dateBtn.getAttribute('data-date') || 'all' : 'all';
        var selCinema = cinemaBtn ? cinemaBtn.getAttribute('data-cinema') || 'all' : 'all';
        var selSaver = saverBtn ? saverBtn.getAttribute('data-saver') || 'all' : 'all';
        var sectionVis = { now: false, coming: false };
        var cards = filmsEl.querySelectorAll('.film-card');
        for (var i = 0; i < cards.length; i++) {
          var card = cards[i];
          var scriptEl = card.querySelector('script.film-showtimes-full');
          if (!scriptEl) continue;
          var rows;
          try {
            rows = parseShowtimesJson(scriptEl.textContent.trim());
          } catch (e2) {
            card.style.removeProperty('display');
            var stBad = card.getAttribute('data-status') || '';
            if (stBad === 'now') sectionVis.now = true;
            if (stBad === 'coming-soon') sectionVis.coming = true;
            continue;
          }
          var picked = [];
          for (var ri = 0; ri < rows.length; ri++) {
            if (rowMatches(rows[ri], selDate, selCinema, selSaver)) picked.push(rows[ri]);
          }
          if (picked.length === 0) {
            card.style.display = 'none';
            continue;
          }
          card.style.removeProperty('display');
          var optsKey = card.getAttribute('data-options-key') || 'film';
          var wrap = card.querySelector('.showtimes');
          if (wrap) wrap.innerHTML = buildShowtimesInner(picked, optsKey);
          var status = card.getAttribute('data-status') || '';
          if (status === 'now') sectionVis.now = true;
          if (status === 'coming-soon') sectionVis.coming = true;
        }
        document.querySelectorAll('.film-section').forEach(function(section) {
          var sectionType = section.getAttribute('data-section') || '';
          var showSection = sectionType === 'now' ? sectionVis.now : sectionVis.coming;
          section.style.display = showSection ? '' : 'none';
        });
      }

      function activateTabRow(selector, btn) {
        document.querySelectorAll(selector).forEach(function(b) { b.classList.remove('active'); });
        btn.classList.add('active');
      }

      document.querySelectorAll('.tab-date').forEach(function(btn) {
        btn.addEventListener('click', function() {
          activateTabRow('.tab-date', btn);
          applyFilters();
        });
      });
      document.querySelectorAll('.tab-cinema').forEach(function(btn) {
        btn.addEventListener('click', function() {
          activateTabRow('.tab-cinema', btn);
          var c = btn.getAttribute('data-cinema') || 'all';
          try {
            if (c === 'all') localStorage.removeItem(storageKey);
            else localStorage.setItem(storageKey, c);
          } catch (e3) {}
          applyFilters();
        });
      });
      document.querySelectorAll('.tab-saver').forEach(function(btn) {
        btn.addEventListener('click', function() {
          activateTabRow('.tab-saver', btn);
          applyFilters();
        });
      });

      filmsEl.addEventListener('click', function(ev) {
        var btn = ev.target && ev.target.closest ? ev.target.closest('.showtimes-more-btn') : null;
        if (!btn || !filmsEl.contains(btn)) return;
        var targetId = btn.getAttribute('data-target');
        var target = targetId ? document.getElementById(targetId) : null;
        if (!target) return;
        var isHidden = target.hasAttribute('hidden');
        if (isHidden) {
          target.removeAttribute('hidden');
          btn.textContent = btn.getAttribute('data-less-label') || 'Show fewer showings';
        } else {
          target.setAttribute('hidden', '');
          btn.textContent = btn.getAttribute('data-more-label') || 'Show more showings';
        }
      });

      try {
        var saved = localStorage.getItem(storageKey);
        if (saved) {
          var tabs = document.querySelectorAll('.tab-cinema');
          var found = false;
          for (var si = 0; si < tabs.length; si++) {
            if (tabs[si].getAttribute('data-cinema') === saved) {
              activateTabRow('.tab-cinema', tabs[si]);
              found = true;
              break;
            }
          }
          if (!found) localStorage.removeItem(storageKey);
        }
      } catch (e4) {}
      applyFilters();
    })();
    document.querySelectorAll('.cast-more-btn').forEach(function(btn) {
      btn.addEventListener('click', function() {
        var rest = btn.previousElementSibling;
        if (rest && rest.classList.contains('cast-rest')) {
          var on = rest.hasAttribute('hidden');
          if (on) { rest.removeAttribute('hidden'); btn.textContent = 'Less'; }
          else { rest.setAttribute('hidden', ''); btn.textContent = 'More'; }
        }
      });
    });
    (function() {
      var lb = document.getElementById('trailer-lightbox');
      var iframe = document.getElementById('trailer-lightbox-iframe');
      var backdrop = document.getElementById('trailer-lightbox-backdrop');
      var closeBtn = document.getElementById('trailer-lightbox-close');
      var fallbackLink = document.getElementById('trailer-lightbox-fallback');
      function closeLightbox() {
        lb.classList.remove('is-open');
        lb.setAttribute('aria-hidden', 'true');
        iframe.src = '';
        if (fallbackLink) fallbackLink.href = '#';
      }
      function openLightbox(embedUrl, watchUrl) {
        iframe.src = embedUrl;
        if (fallbackLink && watchUrl) fallbackLink.href = watchUrl;
        lb.classList.add('is-open');
        lb.setAttribute('aria-hidden', 'false');
      }
      document.querySelectorAll('.trailer-lightbox-trigger').forEach(function(btn) {
        btn.addEventListener('click', function() {
          var embedUrl = this.getAttribute('data-embed');
          var watchUrl = this.getAttribute('data-watch') || '';
          if (embedUrl) openLightbox(embedUrl, watchUrl);
        });
      });
      if (backdrop) backdrop.addEventListener('click', closeLightbox);
      if (closeBtn) closeBtn.addEventListener('click', closeLightbox);
      document.addEventListener('keydown', function(e) {
        if (e.key === 'Escape' && lb.classList.contains('is-open')) closeLightbox();
      });
    })();
    (function() {
      var modal = document.getElementById('film-page-modal');
      var modalList = document.getElementById('film-page-modal-list');
      var modalBackdrop = document.getElementById('film-page-modal-backdrop');
      var modalClose = document.getElementById('film-page-modal-close');
      function closeCinemaModal() {
        modal.classList.remove('is-open');
        modal.setAttribute('aria-hidden', 'true');
        modalList.innerHTML = '';
      }
      document.querySelectorAll('.film-page-trigger').forEach(function(btn) {
        btn.addEventListener('click', function() {
          var optionsId = this.getAttribute('data-options-id');
          var options = document.getElementById(optionsId);
          if (!options) return;
          modalList.innerHTML = options.innerHTML || '';
          modal.classList.add('is-open');
          modal.setAttribute('aria-hidden', 'false');
        });
      });
      if (modalBackdrop) modalBackdrop.addEventListener('click', closeCinemaModal);
      if (modalClose) modalClose.addEventListener('click', closeCinemaModal);
      document.addEventListener('keydown', function(e) {
        if (e.key === 'Escape' && modal.classList.contains('is-open')) closeCinemaModal();
      });
    })();
    function switchView(view) {
      var pageEl = document.querySelector('.page');
      if (!pageEl) return;
      document.querySelectorAll('.view-btn').forEach(function(b) { b.classList.toggle('active', b.dataset.view === view); });
      pageEl.classList.toggle('poster-view', view === 'posters');
    }
