import re
import streamlit as st
from subscription_manager import (
    add_subscription,
    get_subscriptions_for_email,
    is_sheets_configured,
    remove_subscription,
)


def is_valid_email(email: str) -> bool:
    return bool(re.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", email))


def show_alerts_ui():
    st.markdown("---")
    st.markdown("## Email Alerts")

    st.markdown(
        "Subscribe to receive an email notification whenever new political ads are detected."
    )

    with st.expander("Create a new alert", expanded=True):
        col1, col2 = st.columns(2)
        with col1:
            alert_email = st.text_input("Your email address", key="alert_email")
            alert_advertiser = st.text_input(
                "Advertiser keyword",
                key="alert_advertiser",
            )
        with col2:
            alert_geo = st.text_input(
                "Geography (state name or abbreviation)",
                key="alert_geo",
            )
            alert_platforms = st.multiselect(
                "Platforms to monitor",
                ["Google", "Meta", "X"],
                default=["Google", "Meta", "X"],
                key="alert_platforms",
            )

        if st.button("Subscribe", key="subscribe_btn"):
            if not alert_email or not is_valid_email(alert_email):
                st.error("Please enter a valid email address.")
            elif not alert_advertiser and not alert_geo:
                st.error("Please specify at least an advertiser keyword or a geography.")
            elif not alert_platforms:
                st.error("Please select at least one platform.")
            else:
                sub_id = add_subscription(
                    email=alert_email,
                    advertiser_keyword=alert_advertiser,
                    geography=alert_geo,
                    platforms=alert_platforms,
                )
                if sub_id:
                    st.success(
                        f"Alert created! You'll receive emails at **{alert_email}** "
                        f"when new ads matching your criteria appear."
                    )
                else:
                    st.warning("You already have an identical alert set up.")

    st.markdown("## Manage your alerts")
    lookup_email = st.text_input("Enter your email to view/remove alerts", key="lookup_email")

    if lookup_email:
        if not is_valid_email(lookup_email):
            st.error("Please enter a valid email address.")
        else:
            subs = get_subscriptions_for_email(lookup_email)
            if not subs:
                st.info("No alerts found for this email.")
            else:
                for sub in subs:
                    with st.container():
                        col_a, col_b = st.columns([4, 1])
                        with col_a:
                            parts = []
                            if sub.get("advertiser_keyword"):
                                parts.append(f"**Advertiser:** {sub['advertiser_keyword']}")
                            if sub.get("geography"):
                                parts.append(f"**Geography:** {sub['geography']}")
                            parts.append(f"**Platforms:** {', '.join(sub.get('platforms', []))}")
                            if sub.get("last_notified_at"):
                                parts.append(f"**Last notified:** {sub['last_notified_at'][:19]}")
                            st.markdown(" · ".join(parts))
                        with col_b:
                            if st.button("🗑️ Remove", key=f"remove_{sub['id']}"):
                                remove_subscription(sub["id"])
                                st.success("Alert removed.")
                                st.rerun()