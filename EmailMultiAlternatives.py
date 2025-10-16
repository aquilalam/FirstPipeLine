import os
import logging
from django.contrib import messages
from django.shortcuts import get_object_or_404, redirect
from django.views.decorators.http import require_POST
from django.http import JsonResponse
from django.utils import timezone
from django.db import transaction
from django.core.mail import EmailMultiAlternatives
from django.template.loader import render_to_string
from django.utils.html import strip_tags
from django.conf import settings
import mimetypes

from apps.bluebird.models import Section, Entry, EntryField, DocumentMetadata
from apps.bluebird.services.infrastructure import _send_file_to_s3
from apps.bluebird.services import workflow_services as wf

logger = logging.getLogger(__name__)


def send_correspondence_email(request, module_context, email_id):
    """
    Send an email correspondence and update its status to 'Sent'.
    """
    try:
        # Get the email correspondence entry
        email_entry = Entry.objects.get(
            id=email_id, section__key="purge/internal/email"
        )

        # Get the email details from entry fields
        email_fields = {field.key: field.value for field in email_entry.fields.all()}

        # Validate required fields for sending
        required_fields = ["recipient_email", "subject", "email_date", "email_type"]
        missing_fields = []
        for field in required_fields:
            if not email_fields.get(field):
                missing_fields.append(field.replace("_", " ").title())

        if missing_fields:
            error_msg = f"Cannot send email. Missing required fields: {', '.join(missing_fields)}"

            # Return JSON for AJAX requests
            if request.headers.get("Content-Type") == "application/json":
                return JsonResponse({"success": False, "error": error_msg})

            # Fallback to original redirect behavior
            messages.error(request, error_msg)
            purge_record_id = email_fields.get("purge_record_id")
            if purge_record_id:
                return redirect(
                    "bluebird:view_email_correspondence_for_purge",
                    purge_record_id=purge_record_id,
                )
            return redirect("bluebird:view_internal_purge_records")

        # Check if email is in Draft status
        current_status = email_fields.get("status", "")
        if current_status != "Draft":
            error_msg = f"Email cannot be sent. Current status: {current_status}. Only Draft emails can be sent."

            # Return JSON for AJAX requests
            if request.headers.get("Content-Type") == "application/json":
                return JsonResponse({"success": False, "error": error_msg})

            # Fallback to original redirect behavior
            messages.warning(request, error_msg)
            purge_record_id = email_fields.get("purge_record_id")
            if purge_record_id:
                return redirect(
                    "bluebird:view_email_correspondence_for_purge",
                    purge_record_id=purge_record_id,
                )
            return redirect("bluebird:view_internal_purge_records")

        # === ACTUAL EMAIL SENDING LOGIC ===
        try:
            recipient_email = email_fields.get("recipient_email", "")
            subject = email_fields.get("subject", "")
            body_content = email_fields.get("body_content", "")
            sender_email = email_fields.get("sender_email", settings.DEFAULT_FROM_EMAIL)

            # Get CC and BCC if they exist
            cc_emails = email_fields.get("cc_emails", "")
            bcc_emails = email_fields.get("bcc_emails", "")

            # Parse CC and BCC (assume comma-separated)
            cc_list = [email.strip() for email in cc_emails.split(",") if email.strip()] if cc_emails else []
            bcc_list = [email.strip() for email in bcc_emails.split(",") if email.strip()] if bcc_emails else []

            # Get purge record information for context
            purge_record_id = email_fields.get("purge_record_id")
            purge_info = {}
            if purge_record_id:
                try:
                    purge_record = Entry.objects.get(
                        id=purge_record_id,
                        section__key__in=["purge/internal", "purge/ic"]
                    )
                    purge_fields = {field.key: field.value for field in purge_record.fields.all()}
                    purge_info = {
                        "id": purge_record_id,
                        "account": purge_fields.get("account", "N/A"),
                        "provider": purge_fields.get("provider", "N/A"),
                        "facility": purge_fields.get("facility", "N/A"),
                        "status": purge_fields.get("status", "N/A"),
                    }
                except Entry.DoesNotExist:
                    pass

            # Prepare email context for template
            email_context = {
                "subject": subject,
                "body_content": body_content,
                "email_type": email_fields.get("email_type", ""),
                "priority": email_fields.get("priority", "Normal"),
                "purge_info": purge_info,
                "sender_name": request.user.get_full_name() or request.user.username,
                "email_date": email_fields.get("email_date", timezone.now().strftime("%Y-%m-%d")),
            }

            # Create HTML email from template (if template exists)
            try:
                html_content = render_to_string(
                    "emails/correspondence_email.html",
                    email_context
                )
                # Create plain text version by stripping HTML
                text_content = strip_tags(body_content)
            except Exception as template_error:
                # Fallback to plain text if template doesn't exist
                logger.warning(f"Email template not found, using plain text: {template_error}")
                html_content = f"""
                <html>
                    <body>
                        <div style="font-family: Arial, sans-serif; padding: 20px;">
                            <h2>{subject}</h2>
                            <div style="white-space: pre-wrap;">{body_content}</div>
                            <hr style="margin: 20px 0;">
                            <p style="color: #666; font-size: 12px;">
                                This email was sent via the Purge Workflow System<br>
                                Email Type: {email_fields.get("email_type", "N/A")}<br>
                                Priority: {email_fields.get("priority", "Normal")}<br>
                                Sent by: {request.user.get_full_name() or request.user.username}
                            </p>
                        </div>
                    </body>
                </html>
                """
                text_content = body_content

            # Create the email message
            email_message = EmailMultiAlternatives(
                subject=subject,
                body=text_content,
                from_email=sender_email,
                to=[recipient_email],
                cc=cc_list if cc_list else None,
                bcc=bcc_list if bcc_list else None,
                reply_to=[sender_email] if sender_email != settings.DEFAULT_FROM_EMAIL else None,
            )

            # Attach HTML version
            email_message.attach_alternative(html_content, "text/html")

            # Attach files from DocumentMetadata
            attachments = DocumentMetadata.objects.filter(
                entry=email_entry,
                is_deleted=False
            )

            attachment_errors = []
            for attachment in attachments:
                try:
                    # Download file from S3
                    import boto3
                    from botocore.exceptions import ClientError

                    s3_client = boto3.client('s3')
                    bucket_name = os.environ.get("BLUEBIRD_FILES_BUCKET")

                    # Get file from S3
                    s3_response = s3_client.get_object(
                        Bucket=bucket_name,
                        Key=attachment.s3_key
                    )
                    file_content = s3_response['Body'].read()

                    # Determine MIME type
                    mime_type, _ = mimetypes.guess_type(attachment.filename)
                    if not mime_type:
                        mime_type = 'application/octet-stream'

                    # Attach to email
                    email_message.attach(
                        attachment.filename,
                        file_content,
                        mime_type
                    )

                except ClientError as s3_error:
                    error_detail = f"S3 error for {attachment.filename}: {str(s3_error)}"
                    logger.error(error_detail)
                    attachment_errors.append(error_detail)
                except Exception as attach_error:
                    error_detail = f"Failed to attach {attachment.filename}: {str(attach_error)}"
                    logger.error(error_detail)
                    attachment_errors.append(error_detail)

            # Send the email
            try:
                email_message.send(fail_silently=False)
                email_sent_successfully = True
                send_error = None
            except Exception as send_error_obj:
                email_sent_successfully = False
                send_error = str(send_error_obj)
                logger.error(f"Failed to send email: {send_error}")

            # If email sending failed, return error
            if not email_sent_successfully:
                error_msg = f"Failed to send email: {send_error}"

                if request.headers.get("Content-Type") == "application/json":
                    return JsonResponse({"success": False, "error": error_msg})

                messages.error(request, error_msg)
                purge_record_id = email_fields.get("purge_record_id")
                if purge_record_id:
                    return redirect(
                        "bluebird:view_email_correspondence_for_purge",
                        purge_record_id=purge_record_id,
                    )
                return redirect("bluebird:view_internal_purge_records")

        except Exception as email_prep_error:
            error_msg = f"Error preparing email: {str(email_prep_error)}"
            logger.error(error_msg)

            if request.headers.get("Content-Type") == "application/json":
                return JsonResponse({"success": False, "error": error_msg})

            messages.error(request, error_msg)
            purge_record_id = email_fields.get("purge_record_id")
            if purge_record_id:
                return redirect(
                    "bluebird:view_email_correspondence_for_purge",
                    purge_record_id=purge_record_id,
                )
            return redirect("bluebird:view_internal_purge_records")

        # === UPDATE DATABASE AFTER SUCCESSFUL SEND ===
        # Update status and sent_date using bulk operations
        fields_to_update_bulk = []
        fields_to_create = []
        existing_fields = {field.key: field for field in email_entry.fields.all()}

        # Handle status field
        if "status" in existing_fields:
            status_field = existing_fields["status"]
            status_field.value = "Sent"
            fields_to_update_bulk.append(status_field)
        else:
            fields_to_create.append(
                EntryField(entry=email_entry, key="status", value="Sent")
            )

        # Handle sent_date field
        sent_date_value = timezone.now().isoformat()
        if "sent_date" in existing_fields:
            sent_date_field = existing_fields["sent_date"]
            sent_date_field.value = sent_date_value
            fields_to_update_bulk.append(sent_date_field)
        else:
            fields_to_create.append(
                EntryField(entry=email_entry, key="sent_date", value=sent_date_value)
            )

        # Add sent_by field
        if "sent_by" in existing_fields:
            sent_by_field = existing_fields["sent_by"]
            sent_by_field.value = request.user.username
            fields_to_update_bulk.append(sent_by_field)
        else:
            fields_to_create.append(
                EntryField(entry=email_entry, key="sent_by", value=request.user.username)
            )

        # Perform all database operations atomically
        with transaction.atomic():
            # Bulk create new fields
            if fields_to_create:
                EntryField.objects.bulk_create(fields_to_create)

            # Bulk update existing fields
            if fields_to_update_bulk:
                EntryField.objects.bulk_update(fields_to_update_bulk, ["value"])

            # Update entry modified timestamp with specific field update
            email_entry.modified_at = timezone.now()
            email_entry.save(update_fields=["modified_at"])

        # Success response
        success_msg = f"Email '{subject}' has been sent successfully to {recipient_email}."

        # Add warning if there were attachment errors
        if attachment_errors:
            warning_msg = f"Email sent, but some attachments failed: {'; '.join(attachment_errors[:3])}"
            if len(attachment_errors) > 3:
                warning_msg += f" and {len(attachment_errors) - 3} more..."
            logger.warning(warning_msg)

            if request.headers.get("Content-Type") == "application/json":
                return JsonResponse(
                    {
                        "success": True,
                        "message": success_msg,
                        "warning": warning_msg,
                        "status": "Sent",
                        "sent_date": timezone.now().isoformat(),
                    }
                )
            messages.success(request, success_msg)
            messages.warning(request, warning_msg)
        else:
            # Return JSON for AJAX requests
            if request.headers.get("Content-Type") == "application/json":
                return JsonResponse(
                    {
                        "success": True,
                        "message": success_msg,
                        "status": "Sent",
                        "sent_date": timezone.now().isoformat(),
                    }
                )
            messages.success(request, success_msg)

        # Redirect back to the email correspondence view
        purge_record_id = email_fields.get("purge_record_id")
        if purge_record_id:
            return redirect(
                "bluebird:view_email_correspondence_for_purge",
                purge_record_id=purge_record_id,
            )

    except Entry.DoesNotExist:
        error_msg = f"Email correspondence with ID {email_id} not found."

        # Return JSON for AJAX requests
        if request.headers.get("Content-Type") == "application/json":
            return JsonResponse({"success": False, "error": error_msg})

        # Fallback to original redirect behavior
        messages.error(request, error_msg)
    except Exception as e:
        error_msg = f"Error sending email: {str(e)}"
        logger.error(f"Unexpected error in send_correspondence_email: {str(e)}", exc_info=True)

        # Return JSON for AJAX requests
        if request.headers.get("Content-Type") == "application/json":
            return JsonResponse({"success": False, "error": error_msg})

        # Fallback to original redirect behavior
        messages.error(request, error_msg)

    return redirect("bluebird:view_internal_purge_records")